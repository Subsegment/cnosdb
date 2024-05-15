//
// const ARG_PRINT: &str = "print"; // To print something
// const ARG_REPAIR: &str = "repair"; // To repair something
// const ARG_TSM: &str = "--tsm"; // To print a .tsm file
// const ARG_TOMBSTONE: &str = "--tombstone"; // To print a .tsm file with tombsotne
// const ARG_SUMMARY: &str = "--summary"; // To print a summary file
// const ARG_WAL: &str = "--wal"; // To print a wal file
// const ARG_INDEX: &str = "--index"; // To print a wal file

use std::collections::HashMap;
use std::fmt::Write;
use std::time::Duration;

/// # Example
/// tskv print [--tsm <tsm_path>] [--tombstone]
/// tskv print [--summary <summary_path>]
/// tskv print [--wal <wal_path>]
/// tskv repair [--index <file_name>]
/// - --tsm <tsm_path> print statistics for .tsm file at <tsm_path> .
/// - --tombstone also print tombstone for every field_id in .tsm file.
// #[tokio::main]
// async fn main() {
//     let mut args = env::args().peekable();
//
//     let mut show_tsm = false;
//     let mut tsm_path: Option<String> = None;
//     let mut show_tombstone = false;
//
//     let mut show_summary = false;
//     let mut summary_path: Option<String> = None;
//
//     let mut show_wal = false;
//     let mut wal_path: Option<String> = None;
//
//     let mut repair_index = false;
//     let mut index_file: Option<String> = None;
//
//     while let Some(arg) = args.peek() {
//         // --print [--tsm <path>]
//         if arg.as_str() == ARG_PRINT {
//             while let Some(print_arg) = args.next() {
//                 match print_arg.as_str() {
//                     ARG_TSM => {
//                         show_tsm = true;
//                         tsm_path = args.next();
//                         if tsm_path.is_none() {
//                             println!("Invalid arguments: --tsm <tsm_path>");
//                         }
//                     }
//                     ARG_TOMBSTONE => {
//                         show_tombstone = true;
//                     }
//                     ARG_SUMMARY => {
//                         show_summary = true;
//                         summary_path = args.next();
//                         if summary_path.is_none() {
//                             println!("Invalid arguments: --summary <summary_path>")
//                         }
//                     }
//                     ARG_WAL => {
//                         show_wal = true;
//                         wal_path = args.next();
//                         if wal_path.is_none() {
//                             println!("Invalid arguments: --wal <wal_path>")
//                         }
//                     }
//                     _ => {}
//                 }
//             }
//         } else if arg.as_str() == ARG_REPAIR {
//             while let Some(repair_arg) = args.next() {
//                 if repair_arg.as_str() == ARG_INDEX {
//                     repair_index = true;
//                     index_file = args.next();
//                     if index_file.is_none() {
//                         println!("Invalid arguments: --index <index file>");
//                     }
//                 }
//             }
//         }
//         args.next();
//     }
//
//     if show_tsm {
//         if let Some(p) = tsm_path {
//             println!("TSM Path: {}, ShowTombstone: {}", p, show_tombstone);
//             // tskv::print_tsm_statistics(p, show_tombstone).await;
//         }
//     }
//
//     if show_summary {
//         if let Some(p) = summary_path {
//             println!("Summary Path: {}", p);
//             tskv::print_summary_statistics(p).await;
//         }
//     }
//
//     if show_wal {
//         if let Some(p) = wal_path {
//             println!("Wal Path: {}", p);
//             tskv::print_wal_statistics(p).await;
//         }
//     }
//
//     if repair_index {
//         if let Some(name) = index_file {
//             println!("repair index: {}", name);
//             let result = tskv::index::binlog::repair_index_file(&name).await;
//             println!("repair index result: {:?}", result);
//         }
//     }
// }

//
// # Generate JSON data like this:
// # {
// #     "timestamp": "2024/04/01 09:44:03.900",
// #     "uploadDatetime": "2024/04/01 09:44:03.900",
// #     "size": "HZ",
// #     "resource": "WXXX11DV",
// #     "MESIP": "10.229.169.171",
// #     "ProjectName": "CP",
// #     "ProcessesParamKV": [
// #         {
// #             "ANONE_TENSION_001_Upper": "800",
// #             "ANONE_TENSION_001_Lower": "700",
// #             "ANONE_TENSION_001_Setting": "700",
// #             "CATHODE_TENSION_001_Upper": "800",
// #             "CATHODE_TENSION_001_Lower": "700",
// #             "CATHODE_TENSION_001_Setting": "700",
// #             "JES_001_Setting": "23",
// #             "JRSD_001_Upper": "2500",
// #             "JRSD_001_Lower": "100",
// #             "JRSD_001_Setting": "2500",
// #             "JRYYSJ_001_Upper": "10",
// #             "JRYYSJ_001_Lower": "4",
// #             "JRYYSJ_001_Setting": "6",
// #             "OH_OF_TAIL_LENGTHTAPE_001_Upper": "20",
// #             "OH_OF_TAIL_LENGTHTAPE_001_Lower": "3",
// #             "PATTERNGQGQY_001_Upper": "0.5",
// #             "PATTERNGQGQY_001_Lower": "0"
// #         }
// #     ]
// # }
use chrono::{TimeZone, Utc};
use clap::{Args, Parser, Subcommand};
use rand::Rng;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use reqwest::ClientBuilder;
use serde::{Deserialize, Serialize};
use serde_json::{Result, Value};
use trace::info;

#[derive(Serialize, Deserialize, Default)]
struct Simulator {
    #[serde(skip)]
    start_ts: i64,
    #[serde(skip)]
    end_ts: i64,
    #[serde(skip)]
    series_num: u64,
    #[serde(skip)]
    epoch_timestamp: i64,
    #[serde(skip)]
    interval_seconds: i64,
    #[serde(skip)]
    params_num: usize,
    #[serde(skip)]
    param_key_prefix: String,
    #[serde(skip)]
    param_key_names: Vec<String>,
    #[serde(skip)]
    param_val_bounds: (i32, i32),
    timestamp: String,
    upload_date_time: String,
    size: i32,
    resource: String,
    mes_ip: String,
    project_name: String,
    processes_param_kv: Vec<HashMap<String, i32>>,
}

impl Simulator {
    fn update_timestamp(&mut self) {
        let dt = Utc.timestamp(self.epoch_timestamp, 0);
        self.timestamp = dt.to_rfc3339();
        self.upload_date_time = dt.format("%Y-%m-%d %H:%M:%S").to_string();
    }

    fn get_key(&self) -> String {
        format!(
            "{}_{}_{}_{}",
            self.project_name, self.size, self.mes_ip, self.resource
        )
    }

    fn gen(&mut self, id: u64) {
        let mut rng = rand::thread_rng();
        self.update_timestamp();
        self.size = id as i32;
        self.resource = format!("WXXX11DV-{}", id);
        self.mes_ip = format!("192.168.{}.{}", id, id);
        self.project_name = format!("Project_{}", id);
        // 400 column key  400 * 3 1200  column value
        let mut col_set = HashMap::new();
        let epoch = self.params_num + rng.gen_range(0..10);
        for i in 0..epoch {
            let param_key = format!(
                "{}-{}-{}",
                self.param_key_prefix,
                i + 1,
                "Lower".to_string()
            );
            col_set.insert(param_key, self.param_val_bounds.0);
            let param_key = format!(
                "{}-{}-{}",
                self.param_key_prefix,
                i + 1,
                "Upper".to_string()
            );
            col_set.insert(param_key, self.param_val_bounds.1);
            let param_key = format!(
                "{}-{}-{}",
                self.param_key_prefix,
                i + 1,
                "Setting".to_string()
            );
            let val = rng.gen_range(self.param_val_bounds.0..self.param_val_bounds.1);
            col_set.insert(param_key, val);
        }
        self.processes_param_kv.push(col_set);
    }

    fn to_json(&self) -> Result<Value> {
        serde_json::to_value(self)
    }

    fn to_lineprotocol(&self) -> String {
        let mut line = format!(
            "tskv,project_name={},resource={},mes_ip={},size={} ",
            self.project_name, self.resource, self.mes_ip, self.size
        );
        for (k, v) in self.processes_param_kv[0].iter() {
            line.push_str(&format!("{}={},", k, v));
        }
        line.pop();
        // write!(line," {}", self.epoch_timestamp);
        line.push_str(&format!(" {}", self.epoch_timestamp));
        line
    }
}

pub struct PayLoad {
    key: String,
    load: String,
}

pub struct KafkaProducer {
    brokers: String,
    topic_name: String,
    timeout_ms: u64,
}

impl KafkaProducer {
    pub fn new(brokers: &str, topic_name: &str, timeout_ms: u64) -> Self {
        KafkaProducer {
            brokers: brokers.to_string(),
            topic_name: topic_name.to_string(),
            timeout_ms,
        }
    }
    async fn produce(&self, payloads: Vec<PayLoad>) {
        let producer: &FutureProducer = &ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("message.timeout.ms", self.timeout_ms.to_string().as_str())
            .create()
            .expect("Producer creation error");

        let futures = payloads
            .iter()
            .map(|payload| async move {
                let delivery_status = producer
                    .send(
                        FutureRecord::to(&*self.topic_name)
                            .payload(&payload.load)
                            .key(&payload.key),
                        Duration::from_secs(self.timeout_ms),
                    )
                    .await;
                delivery_status
            })
            .collect::<Vec<_>>();

        // This loop will wait until all delivery statuses have been received.
        for future in futures {
            info!("Future completed. Result: {:?}", future.await);
        }
    }
}

pub struct KafkaConsumer {
    brokers: String,
    topic_name: String,
    timeout_ms: u64,
}

impl KafkaConsumer {
    pub fn new(brokers: &str, topic_name: &str, timeout_ms: u64) -> Self {
        KafkaConsumer {
            brokers: brokers.to_string(),
            topic_name: topic_name.to_string(),
            timeout_ms,
        }
    }
}
#[derive(Debug, Parser)]
#[command(about = "CnosDB command line tools")]
#[command(long_about = r#"CnosDB and command line tools
Examples:
    # Run the CnosDB:
    cnosdb run --addr "127.0.0.1:8902" --db "public"#)]
struct Cli {
    #[command(subcommand)]
    subcmd: CliCommand,
}

#[derive(Debug, Subcommand)]
enum CliCommand {
    Run(RunArgs),
}

#[derive(Debug, Args)]
struct RunArgs {
    /// Run mode, the default value is "cnosdb"
    #[arg(long)]
    mode: String,

    /// Number of CPUs on the system, the default value is 4
    #[arg(long, global = true)]
    addr: Option<String>,

    /// Gigabytes(G) of memory on the system, the default value is 16
    #[arg(long, global = true)]
    db: Option<String>,

    #[arg(long, global = true)]
    user_name: Option<String>,

    #[arg(long, global = true)]
    password: Option<String>,

    #[arg(long, global = true)]
    start_ts: Option<i64>,

    #[arg(long, global = true)]
    end_ts: Option<i64>,

    #[arg(long, global = true)]
    series_num: Option<u64>,

    #[arg(long, global = true)]
    interval_seconds: Option<i64>,

    #[arg(long, global = true)]
    params_num: Option<usize>,

    #[arg(long, global = true)]
    params_upper_bound: Option<i32>,

    #[arg(long, global = true)]
    params_lower_bound: Option<i32>,

    #[arg(long, global = true)]
    buffer_size: Option<usize>,

    #[arg(long, global = true)]
    all_data_size: Option<u64>,
}

pub enum Mode {
    Influxdb,
    Cnosdb,
}

pub struct Client {}

impl Client {
    pub async fn write_line_to_cnosdb(
        addr: &str,
        db: &str,
        username: &str,
        _password: &str,
        body: Vec<u8>,
    ) {
        let url = format!("http://{}/api/v1/write?db={}", addr, db);
        let client = ClientBuilder::new().build().unwrap_or_else(|e| {
            panic!("Failed to build http client: {}", e);
        });
        let res = client
            .post(url)
            .basic_auth(username, Option::<&str>::None)
            .body(body)
            .send()
            .await;

        if res.is_err() {
            println!("Failed to write to cnosdb: {}", res.err().unwrap());
        } else {
            let res = res.unwrap();
            if !res.status().is_success() {
                println!("Failed to write to cnosdb: {}", res.status());
            }
        }
    }

    pub async fn write_line_to_influxdb(
        addr: &str,
        db: &str,
        username: &str,
        password: &str,
        body: Vec<u8>,
    ) {
        let url = format!("http://{}/write?db={}", addr, db);
        let client = ClientBuilder::new().build().unwrap_or_else(|e| {
            panic!("Failed to build http client: {}", e);
        });
        let res = client
            .post(url.clone())
            .header("Authorization", format!("Token {}:{}", username, password))
            .body(body)
            .send()
            .await;

        if res.is_err() {
            println!("Failed to write to influxdb: {}", res.err().unwrap());
        } else {
            let res = res.unwrap();
            if !res.status().is_success() {
                println!("Failed to write to influxdb: {}", res.status());
            }
        }
    }
}
#[tokio::main]
async fn main() {
    // let brokers = "localhost:9092";
    // let topic_name = "test";
    let cli = Cli::parse();
    let run_args = match cli.subcmd {
        CliCommand::Run(run_args) => run_args,
    };
    let mode = match run_args.mode.to_uppercase().as_str() {
        "CNOSDB" => Mode::Cnosdb,
        "INFLUXDB" => Mode::Influxdb,
        _ => {
            println!(
                "Invalid mode: {}, please use influxdb or cnosdb",
                run_args.mode
            );
            return;
        }
    };
    let addr = run_args
        .addr
        .unwrap_or_else(|| "127.0.0.1:8902".to_string());
    let db = run_args.db.unwrap_or_else(|| "public".to_string());
    let start_ts = run_args.start_ts.unwrap_or(0);
    let end_ts = run_args.end_ts.unwrap_or(10000000000000);
    let series_num = run_args.series_num.unwrap_or(3000);
    let interval_seconds = run_args.interval_seconds.unwrap_or(1);
    let params_num = run_args.params_num.unwrap_or(400);
    let params_upper_bound = run_args.params_upper_bound.unwrap_or(200);
    let params_lower_bound = run_args.params_lower_bound.unwrap_or(100);
    let buf_size = run_args.buffer_size.unwrap_or(30 * 1024 * 1024);
    let size = run_args.all_data_size.unwrap_or(100 * 1024 * 1024 * 1024);
    let user_name = run_args.user_name.unwrap_or_else(|| "root".to_string());
    let password = run_args.password.unwrap_or_else(|| String::new());
    println!("addr: {}, db: {}", addr, db);

    let mut simulator = Simulator {
        start_ts,
        end_ts,
        series_num,
        epoch_timestamp: Utc::now().timestamp(),
        interval_seconds,
        params_num,
        param_key_prefix: "SIM".to_string(),
        param_key_names: vec![
            "Upper".to_string(),
            "Lower".to_string(),
            "Setting".to_string(),
        ],
        param_val_bounds: (params_lower_bound, params_upper_bound),
        ..Default::default()
    };
    // let mut payload = Vec::with_capacity(1024);
    let mut ts = simulator.start_ts;
    let mut cur_size: u64 = 0;
    let mut buffer = String::with_capacity(buf_size as usize);
    while ts < simulator.end_ts {
        simulator.epoch_timestamp = ts;
        for id in 0..simulator.series_num {
            simulator.gen(id);
            let res = simulator.to_lineprotocol();
            cur_size += res.len() as u64;
            simulator.processes_param_kv.clear();
            let res = writeln!(&mut buffer, "{res}");
            if res.is_err() {
                println!("Failed to write to buffer: {}", res.err().unwrap());
                continue;
            }
            if buffer.len() > buf_size {
                match mode {
                    Mode::Influxdb => {
                        Client::write_line_to_influxdb(
                            addr.as_str(),
                            db.as_str(),
                            user_name.as_str(),
                            password.as_str(),
                            buffer.clone().into_bytes(),
                        )
                        .await;
                    }
                    Mode::Cnosdb => {
                        Client::write_line_to_cnosdb(
                            addr.as_str(),
                            db.as_str(),
                            user_name.as_str(),
                            password.as_str(),
                            buffer.clone().into_bytes(),
                        )
                        .await;
                    }
                }

                println!("buffer size: {}, total size: {}", buffer.len(), cur_size);
                if cur_size > size {
                    println!("write done total size {}", cur_size);
                    return;
                }

                buffer.clear();
            }
        }
        ts += simulator.interval_seconds;
    }
}
