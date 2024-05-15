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
use rdkafka::message::{Header, OwnedHeaders};
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
        self.size = rng.gen_range(1..100);
        self.resource = format!("WXXX11DV-{}", rng.gen_range(1..10));
        self.mes_ip = format!("192.168.{}.{}", rng.gen_range(1..10), rng.gen_range(1..10));
        self.project_name = format!("Project_{}", rng.gen_range(1..5));
        /// 400 column key  400 * 3 1200  column value
        let mut col_set = HashMap::new();
        for i in 0..self.params_num {
            let param_key = format!(
                "{}-{}-{}-{}",
                self.param_key_prefix,
                id,
                i + 1,
                "Lower".to_string()
            );
            col_set.insert(param_key, self.param_val_bounds.0);
            let param_key = format!(
                "{}-{}-{}-{}",
                self.param_key_prefix,
                id,
                i + 1,
                "Upper".to_string()
            );
            col_set.insert(param_key, self.param_val_bounds.1);
            let param_key = format!(
                "{}-{}-{}-{}",
                self.param_key_prefix,
                id,
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
    /// Run CnosDB server.
    Run(RunArgs),
}

#[derive(Debug, Args)]
struct RunArgs {
    /// Number of CPUs on the system, the default value is 4
    #[arg(short, long, global = true)]
    addr: Option<String>,

    /// Gigabytes(G) of memory on the system, the default value is 16
    #[arg(short, long, global = true)]
    db: Option<String>,
}


pub struct Client {}

impl Client {
    pub async fn write_line(addr: String, db: String, body: Vec<u8>) {
        // let url = "http://127.0.0.1:8902/api/v1/write?db=public";
        let url = format!("http://{}/api/v1/write?db={}", addr, db);
        // let body = "test_v1_write_path,ta=a1,tb=b1 fa=1,fb=2";
        let client = ClientBuilder::new().build().unwrap_or_else(|e| {
            panic!("Failed to build http client: {}", e);
        });
        client
            .post(url)
            .basic_auth("root", Option::<&str>::None)
            .body(body)
            .send()
            .await
            .unwrap();
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
    let addr = run_args.addr.unwrap();
    let db = run_args.db.unwrap();
    println!("addr: {}, db: {}",addr, db);

    let mut simulator = Simulator {
        start_ts: 0,
        end_ts: 10000000000000,
        series_num: 3000,
        epoch_timestamp: Utc::now().timestamp(),
        interval_seconds: 1,
        params_num: 400,
        param_key_prefix: "SIM".to_string(),
        param_key_names: vec![
            "Upper".to_string(),
            "Lower".to_string(),
            "Setting".to_string(),
        ],
        param_val_bounds: (100, 200),
        ..Default::default()
    };
    // let mut payload = Vec::with_capacity(1024);
    let mut ts = simulator.start_ts;
    let size: u64 = 100 * 1024 * 1024 * 1024;
    let buf_size: usize = 30 * 1024 * 1024;
    let mut cur_size:u64 = 0;
    let mut buffer = String::with_capacity(buf_size as usize);
    while ts < simulator.end_ts {
        simulator.epoch_timestamp = ts;
        for id in 0..simulator.series_num {
            simulator.gen(id);
            // let key = simulator.get_key();
            // let json = simulator.to_json().unwrap();
            //build payload
            // println!("key: {}, load: {}", key, json.to_string());
            let res = simulator.to_lineprotocol();
            // println!("{}", res);
            simulator.processes_param_kv.clear();
            writeln!(&mut buffer, "{res}");
            if buffer.len() > buf_size {
                Client::write_line(
                    addr.to_string(),
                    db.to_string(),
                    buffer.clone().into_bytes(),
                )
                .await;
                cur_size += buffer.len() as u64;
                println!("buffer size: {}, total size: {}", buffer.len(), cur_size);
                if cur_size > size {
                    println!("write done total size {}", cur_size);
                    return;
                }


                buffer.clear();
            }

            // http_client.write_line("127.0.0.1:8902", "public", payload);
            // payload.push(PayLoad {
            //     key: key,
            //     load: json.to_string(),
            // });
        }
        ts += simulator.interval_seconds;
    }

    // let producer = KafkaProducer::new(brokers, topic_name, 10000);
    // producer
    //     .produce(payload)
    //     .await;

    // // test consumer
    // let consumer = KafkaConsumer::new(brokers, topic_name, 10000);
}
