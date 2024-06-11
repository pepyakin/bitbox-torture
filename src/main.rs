use clap::Parser;
use rand::RngCore;
use reth_libmdbx::{Environment, EnvironmentFlags, Geometry, Mode, PageSize, WriteFlags};
use std::path::PathBuf;

const GIGABYTE: usize = 1024 * 1024 * 1024;
const TERABYTE: usize = GIGABYTE * 1024;

const PATH: &str = "/mnt/mdbx-torture";

#[derive(Debug, Parser)]
struct Cli {
    #[clap(subcommand)]
    subcmd: SubCommand,

    #[clap(short, long, default_value = PATH)]
    path: String,

    #[clap(short, long)]
    y: bool,

    /// Whether to not sync the database after each batch.
    #[clap(short, long, default_value = "false")]
    yolo: bool,
}

#[derive(Debug, Parser)]
enum SubCommand {
    Fill(FillOpts),
    Stat,
}

impl SubCommand {
    fn as_fill_opts(&self) -> Option<&FillOpts> {
        match self {
            SubCommand::Fill(opts) => Some(opts),
            _ => None,
        }
    }
}

#[derive(Debug, Parser)]
struct FillOpts {
    /// The number of items to insert into the database.
    #[clap(short, long)]
    n: usize,

    /// The size of each batch of items to insert.
    #[clap(short, long, default_value = "1000")]
    batch_sz: usize,

    #[clap(short, long, default_value = "32")]
    value_sz: usize,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.subcmd {
        SubCommand::Fill(_) => fill_database(&cli),
        SubCommand::Stat => stat_database(&cli),
    }
}

fn open(cli: &Cli) -> anyhow::Result<Environment> {
    println!("Opening database, {:?}", cli);
    let env = Environment::builder()
        .set_max_dbs(256)
        .write_map()
        .set_flags(EnvironmentFlags {
            mode: Mode::ReadWrite {
                sync_mode: if cli.yolo {
                    reth_libmdbx::SyncMode::UtterlyNoSync
                } else {
                    reth_libmdbx::SyncMode::Durable
                },
            },
            ..Default::default()
        })
        .set_geometry(Geometry {
            // Maximum database size of 4 terabytes
            size: Some(0..(4 * TERABYTE)),
            // We grow the database in increments of 1 gigabytes
            growth_step: Some(1 * GIGABYTE as isize),
            // The database never shrinks
            shrink_threshold: Some(0),
            page_size: Some(PageSize::Set(4096)),
        })
        .open(&PathBuf::from(&cli.path))?;
    Ok(env)
}

fn stat_database(cli: &Cli) -> anyhow::Result<()> {
    let env = open(cli)?;
    let txn = env.begin_ro_txn().unwrap();
    let main = txn.open_db(Some("main")).unwrap();
    let stat = txn.db_stat(&main).unwrap();

    println!("{:?}", stat);

    Ok(())
}

fn fill_database(cli: &Cli) -> anyhow::Result<()> {
    if std::path::Path::new(&cli.path).exists() {
        if !cli.y {
            eprintln!("Database already exists at {}", &cli.path);
            return Ok(());
        }
        let _ = std::fs::remove_file(&cli.path);
    }

    let fill_ops = cli.subcmd.as_fill_opts().unwrap();
    let env = open(cli)?;

    let txn = env.begin_rw_txn().unwrap();
    let main = txn.create_db(Some("main"), reth_libmdbx::DatabaseFlags::CREATE)?;
    txn.commit()?;

    let mut rand = rand_pcg::Pcg64::new(0xcafef00dd15ea5e5, 0x60e11a7bf9cb254560e11a7bf9cb2545);
    

    let mut remaining = fill_ops.n;
    loop {
        let txn = env.begin_rw_txn().unwrap();

        let start = std::time::Instant::now();
        for _ in 0..fill_ops.batch_sz {
            if remaining == 0 {
                break;
            }
            let mut key: [u8; 32] = [0; 32];
            let mut data = vec![0; fill_ops.value_sz];
            rand.fill_bytes(&mut key);
            rand.fill_bytes(&mut data);
            txn.put(main.dbi(), key, data, WriteFlags::empty()).unwrap();
            remaining -= 1;
        }
        let batch_lat = start.elapsed();
        let stat = txn.db_stat(&main).unwrap();
        let (_, lat) = txn.commit()?;

        if remaining == 0 {
            break;
        }

        println!(
            "Commit {} items in {} ms",
            fill_ops.batch_sz,
            lat.whole().as_millis()
        );
        println!(
            "Inserted {} items in {} ms",
            fill_ops.n - remaining,
            batch_lat.as_millis()
        );
        println!("{:#?}", stat);
    }

    if cli.yolo {
        env.sync(true)?;
    }

    Ok(())
}