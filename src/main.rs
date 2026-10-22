use clap::{Parser, Subcommand};
use crossbeam_channel::unbounded;
use file_transfer::benchmark::run_full_comparison;
use file_transfer::network::receiver::{run_receiver, ReceiverConfig};
use file_transfer::network::sender::{run_sender, SenderConfig, TransferEvent};
use file_transfer::ui::TransferApp;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "file_transfer")]
#[command(about = "Reliable File Transfer over UDP with GUI and Benchmark Mode", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Run in graphical desktop mode (default if no subcommand given)
    #[arg(short, long)]
    gui: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Send a file over UDP to a receiver
    Send {
        /// Path to the file to send
        #[arg(short, long)]
        file: PathBuf,

        /// Target receiver address (e.g. 192.168.43.50:9999)
        #[arg(short, long)]
        target: SocketAddr,

        /// Local bind address (default 0.0.0.0:0)
        #[arg(short, long, default_value = "0.0.0.0:0")]
        bind: SocketAddr,

        /// Chunk payload size in bytes
        #[arg(short, long, default_value = "1400")]
        chunk_size: u32,

        /// Sliding window size in packets
        #[arg(short, long, default_value = "32")]
        window: usize,

        /// Simulate packet loss percentage (0-100)
        #[arg(short, long, default_value = "0")]
        loss: u8,
    },

    /// Run as a receiver listening for incoming files
    Recv {
        /// Destination directory to save received files
        #[arg(short, long, default_value = "./downloads")]
        dest: PathBuf,

        /// Local address to bind (e.g. 0.0.0.0:9999)
        #[arg(short, long, default_value = "0.0.0.0:9999")]
        bind: SocketAddr,

        /// Simulate packet loss percentage (0-100)
        #[arg(short, long, default_value = "0")]
        loss: u8,
    },

    /// Run automated performance benchmark comparing TCP vs Custom UDP
    Benchmark {
        /// Size of test file in megabytes
        #[arg(short, long, default_value = "10")]
        size_mb: usize,
    },
}

fn main() -> eframe::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Send {
            file,
            target,
            bind,
            chunk_size,
            window,
            loss,
        }) => {
            println!("=== Reliable UDP Sender (CLI Mode) ===");
            let (tx, rx) = unbounded();
            let cancel = Arc::new(AtomicBool::new(false));

            let config = SenderConfig {
                file_path: file,
                target_addr: target,
                bind_addr: bind,
                chunk_size,
                window_size: window,
                simulated_loss_percent: loss,
            };

            let cancel_clone = Arc::clone(&cancel);
            let handle = std::thread::spawn(move || {
                run_sender(config, cancel_clone, tx);
            });

            for event in rx {
                match event {
                    TransferEvent::Log(msg) => println!("[LOG] {}", msg),
                    TransferEvent::Progress(st) => {
                        print!(
                            "\r[PROGRESS] {:.1}% | Speed: {} | Chunks: {}/{} | Retransmissions: {}",
                            st.progress_percent(),
                            st.formatted_speed(),
                            st.chunks_transferred,
                            st.total_chunks,
                            st.retransmissions
                        );
                        use std::io::Write;
                        let _ = std::io::stdout().flush();
                    }
                    TransferEvent::Success(msg) => {
                        println!("\n[SUCCESS] {}", msg);
                        break;
                    }
                    TransferEvent::Error(err) => {
                        eprintln!("\n[ERROR] {}", err);
                        break;
                    }
                    TransferEvent::Cancelled => {
                        println!("\n[CANCELLED] Transfer was cancelled.");
                        break;
                    }
                }
            }

            let _ = handle.join();
            Ok(())
        }

        Some(Commands::Recv { dest, bind, loss }) => {
            println!("=== Reliable UDP Receiver (CLI Mode) ===");
            let (tx, rx) = unbounded();
            let cancel = Arc::new(AtomicBool::new(false));

            let config = ReceiverConfig {
                bind_addr: bind,
                destination_dir: dest,
                simulated_loss_percent: loss,
            };

            let cancel_clone = Arc::clone(&cancel);
            let handle = std::thread::spawn(move || {
                run_receiver(config, cancel_clone, tx);
            });

            for event in rx {
                match event {
                    TransferEvent::Log(msg) => println!("[LOG] {}", msg),
                    TransferEvent::Progress(st) => {
                        print!(
                            "\r[PROGRESS] {:.1}% | Speed: {} | Chunks: {}/{} | Drops: {}",
                            st.progress_percent(),
                            st.formatted_speed(),
                            st.chunks_transferred,
                            st.total_chunks,
                            st.corrupt_packets_dropped + st.duplicate_packets_dropped
                        );
                        use std::io::Write;
                        let _ = std::io::stdout().flush();
                    }
                    TransferEvent::Success(msg) => {
                        println!("\n[SUCCESS] {}", msg);
                        break;
                    }
                    TransferEvent::Error(err) => {
                        eprintln!("\n[ERROR] {}", err);
                        break;
                    }
                    TransferEvent::Cancelled => {
                        println!("\n[CANCELLED] Receiver stopped.");
                        break;
                    }
                }
            }

            let _ = handle.join();
            Ok(())
        }

        Some(Commands::Benchmark { size_mb }) => {
            println!("============================================================");
            println!("  COMPUTER NETWORKS PROJECT: UDP vs TCP BENCHMARK EVALUATION");
            println!("============================================================");
            println!("Running empirical file transfer benchmarks for {} MB file...\n", size_mb);

            match run_full_comparison(size_mb) {
                Ok(results) => {
                    println!("\n+--------------------------------+------------+--------------+-----------------+-------------------+");
                    println!("| Protocol                       | Time (sec) | Speed (MB/s) | Retransmissions | SHA-256 Integrity |");
                    println!("+--------------------------------+------------+--------------+-----------------+-------------------+");
                    for r in results {
                        println!(
                            "| {:<30} | {:>10.2} | {:>12.2} | {:>15} | {:>17} |",
                            r.protocol,
                            r.elapsed.as_secs_f64(),
                            r.throughput_mb_s,
                            r.retransmissions,
                            if r.integrity_verified { "VERIFIED OK" } else { "FAILED" }
                        );
                    }
                    println!("+--------------------------------+------------+--------------+-----------------+-------------------+\n");
                }
                Err(e) => {
                    eprintln!("Benchmark failed: {}", e);
                }
            }
            Ok(())
        }

        None => {
            // Default: Launch GUI application
            let native_options = eframe::NativeOptions {
                viewport: eframe::egui::ViewportBuilder::default()
                    .with_inner_size([720.0, 560.0])
                    .with_min_inner_size([600.0, 480.0])
                    .with_title("Reliable UDP File Transfer"),
                ..Default::default()
            };

            eframe::run_native(
                "Reliable UDP File Transfer",
                native_options,
                Box::new(|cc| Ok(Box::new(TransferApp::new(cc)))),
            )
        }
    }
}
