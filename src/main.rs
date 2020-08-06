use influx_db_client::{Client, Point, Value, Precision};
use logwatcher::LogWatcher;
use clap::{App, Arg};
use time::Instant;
use url::Url;
use tokio;

struct InfluxData {
    table: String,
    method: String,
    req_body: String,
    time_query: i64
}

impl InfluxData {
    fn table(line: &str) -> InfluxData {
        let from_vec: Vec<_> = line.match_indices("FROM").collect();
        let where_vec: Vec<_> = line.match_indices("WHERE").collect();
        InfluxData {
            table: line[(from_vec[0].0+from_vec[0].1.len())..where_vec[0].0]
                .to_string(),
            method: String::from("SELECT"),
            req_body: String::from(line),
            time_query: 50
        }
    }
}

fn influx_insert(data: InfluxData) {
    let influx_host = Url::parse("http://example:8088").expect("URL parse");
    let client = Client::new(influx_host, "example_DB")
        .set_authentication("USER", "PASSWORD");
    let point = Point::new("cassandra")
        .add_tag("table", Value::String(String::from(data.table)))
        .add_tag("method", Value::String(String::from(data.method)))
        .add_tag("request", Value::String(String::from(data.req_body)))
        .add_field("value", Value::Integer(data.time_query));

    tokio::runtime::Runtime::new().unwrap().block_on(async move {
        client.write_point(point, Some(Precision::Seconds), None).await.unwrap();        
    });
}

fn file_monitor(file: &str) {
    let mut log_watcher = LogWatcher::register(file.to_string()).unwrap();
    log_watcher.watch(|line: String| {
        if line.contains("timeout") {
            let start = Instant::now();
            let mut data = InfluxData::table(&line);
            let vec_line: Vec<&str> = data.req_body.split_whitespace().collect();
            for (p, i) in vec_line.iter().enumerate() {
                if i.contains("time") {
                    match vec_line[p+1].parse::<i64>() {
                        Ok(n) => data.time_query = n,
                        Err(e) => {
                            println!("can't parse {}", e);
                            continue;
                        }
                    };                    
                    influx_insert(data);
                    let end = Instant::now();
                    println!("{:?} seconds for whatever you did.", end-start);
                    break;
                }          
            }
        }
    });
}

fn main() {
    let read_log_args = App::new("read-file-log")
        .version("0.1 beta")
        .about("tail LOG file -> parse -> output into Influx")
        .author("AKrupin")
        .arg(Arg::with_name("FILE")
            .short("f")
            .long("file")
            .help("Path to log file")
            .required(true)
            .takes_value(true))
        .get_matches();

    if let Some(f) = read_log_args.value_of("FILE") {
        file_monitor(f);
    }
}
