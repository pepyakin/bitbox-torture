use clap::Parser;
use rand::{Rng, RngCore};
use reth_libmdbx::{
    DatabaseFlags, Environment, EnvironmentFlags, Geometry, Mode, PageSize, WriteFlags, RW,
};
use std::{path::PathBuf, rc::Rc, str::FromStr};

const GIGABYTE: usize = 1024 * 1024 * 1024;
const TERABYTE: usize = GIGABYTE * 1024;

const PATH: &str = "/mnt/mdbx-torture";

#[derive(Debug, Copy, Clone)]
enum EngineKind {
    Mdbx,
    Rocksdb,
}

impl FromStr for EngineKind {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "mdbx" => Ok(EngineKind::Mdbx),
            "rocksdb" | "rdb" => Ok(EngineKind::Rocksdb),
            _ => anyhow::bail!("Unknown engine kind: {}", s),
        }
    }
}

#[derive(Debug, Parser)]
struct Cli {
    #[clap(subcommand)]
    subcmd: SubCommand,

    #[clap(short, long)]
    kind: EngineKind,

    #[clap(short, long, default_value = PATH)]
    path: String,

    #[clap(short, long)]
    y: bool,

    /// Whether to continue filling the database if it already exists.
    #[clap(long, default_value = "false")]
    cont: bool,

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

    #[clap(short, long, default_value = "0.3")]
    cold: f32,
}

enum Engine {
    Mdbx(Environment),
    Rocksdb(Rc<rocksdb::DB>),
}

impl Engine {
    fn open(cli: &Cli) -> anyhow::Result<Engine> {
        match cli.kind {
            EngineKind::Mdbx => Engine::open_mdbx(cli),
            EngineKind::Rocksdb => Engine::open_rocksdb(cli),
        }
    }

    fn open_mdbx(cli: &Cli) -> anyhow::Result<Engine> {
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
        Ok(Engine::Mdbx(env))
    }

    fn open_rocksdb(cli: &Cli) -> anyhow::Result<Engine> {
        let db = rocksdb::DB::open_default(&cli.path)?;
        Ok(Engine::Rocksdb(Rc::new(db)))
    }

    fn begin(&self) -> anyhow::Result<Tx> {
        match self {
            Engine::Mdbx(env) => {
                let txn = env.begin_rw_txn()?;
                let db = txn.create_db(None, DatabaseFlags::CREATE)?;
                Ok(Tx::Mdbx { txn, db })
            }
            Engine::Rocksdb(db) => {
                let batch = rocksdb::WriteBatch::default();
                Ok(Tx::Rocksdb {
                    db: db.clone(),
                    batch,
                })
            }
        }
    }

    fn print_stat(&self) -> anyhow::Result<String> {
        match self {
            Engine::Mdbx(env) => {
                let txn = env.begin_ro_txn()?;
                let main = txn.open_db(None).unwrap();
                let stat = txn.db_stat(&main).unwrap();
                Ok(format!("{:?}", stat))
            }
            _ => Ok("".to_string()),
        }
    }
}

enum Tx {
    Mdbx {
        txn: reth_libmdbx::Transaction<RW>,
        db: reth_libmdbx::Database,
    },
    Rocksdb {
        db: Rc<rocksdb::DB>,
        batch: rocksdb::WriteBatch,
    },
}

impl Tx {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()> {
        match self {
            Tx::Mdbx { txn, db } => {
                txn.put(db.dbi(), key, value, WriteFlags::empty())?;
            }
            Tx::Rocksdb { batch, .. } => {
                batch.put(key, value);
            }
        }
        Ok(())
    }

    fn commit(self) -> anyhow::Result<()> {
        match self {
            Tx::Mdbx { txn, .. } => {
                txn.commit()?;
                Ok(())
            }
            Tx::Rocksdb { db, batch } => {
                db.write_without_wal(batch)?; // TODO: write wal = false?
                Ok(())
            }
        }
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.subcmd {
        SubCommand::Fill(_) => fill_database(&cli),
        SubCommand::Stat => stat_database(&cli),
    }
}

fn stat_database(cli: &Cli) -> anyhow::Result<()> {
    let env = Engine::open(cli)?;
    env.print_stat()?;
    Ok(())
}

fn fill_database(cli: &Cli) -> anyhow::Result<()> {
    if std::path::Path::new(&cli.path).exists() {
        if cli.y {
            println!("Database already exists, removing.");
            let _ = std::fs::remove_file(&cli.path);
        } else if cli.cont {
            println!("Database already exists, continuing filling.");
        } else {
            anyhow::bail!("Database already exists, aborting.");
        }
    }

    let fill_ops = cli.subcmd.as_fill_opts().unwrap();

    println!("Opening database, {:?}", cli);
    let env = Engine::open(cli)?;

    let mut rand = rand_pcg::Pcg64::new(0xcafef00dd15ea5e5, 0x60e11a7bf9cb254560e11a7bf9cb2545);

    let mut keys = Vec::with_capacity(fill_ops.n);

    let mut remaining = fill_ops.n;
    loop {
        let mut txn = env.begin().unwrap();

        let start = std::time::Instant::now();
        for _ in 0..fill_ops.batch_sz {
            if remaining == 0 {
                break;
            }

            let key = if keys.is_empty() || rand.gen_bool(fill_ops.cold as f64) {
                let mut key = vec![0; 32];
                rand.fill_bytes(&mut key);
                keys.push(key.clone());
                key
            } else {
                keys[rand.gen_range(0..keys.len())].clone()
            };

            let mut data = vec![0; fill_ops.value_sz];
            rand.fill_bytes(&mut data);
            txn.put(key, data).unwrap();
            remaining -= 1;
        }

        let batch_lat = start.elapsed();
        // let stat = txn.db_stat(&main).unwrap();

        let start = std::time::Instant::now();
        txn.commit()?;
        let commit_lat = start.elapsed();

        if remaining == 0 {
            break;
        }

        println!(
            "Commit {} items in {} ms",
            fill_ops.batch_sz,
            commit_lat.as_millis()
        );
        println!(
            "Inserted {} items in {} ms",
            fill_ops.n - remaining,
            batch_lat.as_millis()
        );
        // println!("{:#?}", stat);
    }

    Ok(())
}
