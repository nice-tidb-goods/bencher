use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

use anyhow::Result;
use clap::Parser;
use log::{error, info, LevelFilter};
use mimalloc::MiMalloc;
use mysql_async::{prelude::*, Pool};
use rand::{prelude::*, rngs::SmallRng, SeedableRng};
use simple_logger::SimpleLogger;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    xdp: bool,

    #[arg(short, long, default_value_t = 1)]
    threads: usize,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    SimpleLogger::new()
        .with_utc_timestamps()
        .with_level(LevelFilter::Warn)
        .init()?;
    let args = Args::parse();
    let pool = Pool::new("mysql://root@172.31.19.92:4000/test");
    for _ in 0..args.threads {
        let mut rng = SmallRng::from_entropy();
        let mut conn = pool.get_conn().await?;
        let stmt = conn.prep("select k from sbtest1 where id = ?").await?;
        if args.xdp {
            conn.exec_drop("set @@tidb_use_xdp = 1", ()).await?;
        }
        tokio::spawn(async move {
            loop {
                let id: i32 = rng.gen_range(1..=1000000);
                if let Err(e) = conn.exec_drop(&stmt, (id,)).await {
                    error!("exec_drop failed: {:?}", e);
                }
                COUNTER.fetch_add(1, Relaxed);
            }
        });
    }
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let count = COUNTER.swap(0, Relaxed);
        info!("{} QPS", count);
    }
}
