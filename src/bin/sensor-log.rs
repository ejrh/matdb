use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use chrono::prelude::*;
use serde::{Serialize, Deserialize};

use matdb::{Dimension, Value, Schema, Transaction, Database, Error};

#[derive(Serialize, Deserialize, Debug)]
struct Sensor {
    id: usize,
    component: String,
    sensor: String,
    kind: String
}

struct Sensors<'s> {
    sensor_array: Vec<Sensor>,
    name_to_pos: HashMap<(&'s str, &'s str, &'s str), usize>,
    next_id: usize
}

impl<'s> Sensors<'s> {
    fn new() -> Sensors<'s> {
        Sensors {
            sensor_array: Vec::new(),
            name_to_pos: HashMap::new(),
            next_id: 1
        }
    }

    fn add_sensor<'f>(&'f mut self, sensor: Sensor) {
        let pos = self.sensor_array.len();
        self.sensor_array.push(sensor);
        let sensor = &self.sensor_array[pos];
        let key = (
            unsafe { std::mem::transmute::<&'f str, &'s str>(sensor.component.as_str()) },
            unsafe { std::mem::transmute::<&'f str, &'s str>(sensor.sensor.as_str()) },
            unsafe { std::mem::transmute::<&'f str, &'s str>(sensor.kind.as_str()) }
        );
        self.name_to_pos.insert(key, pos);
    }

    fn load(&mut self) -> io::Result<()> {
        let mut file = File::open("sensors.json")?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let json = String::from_utf8(buffer).unwrap();
        let sensor_array: Vec<Sensor> = serde_json::from_str(json.as_str())?;

        self.sensor_array.clear();
        self.name_to_pos.clear();

        for sensor in sensor_array {
            if sensor.id >= self.next_id {
                self.next_id = sensor.id + 1;
            }
            self.add_sensor(sensor);
        }

        Ok(())
    }

    fn save(&self) -> io::Result<()> {
        let mut file = File::create("sensors.json")?;
        let json: String = serde_json::to_string_pretty(&self.sensor_array)?;
        file.write_all(json.as_bytes())?;

        Ok(())
    }

    fn get(&mut self, component: &str, sensor: &str, kind: &str) -> usize {
        if let Some(&pos) = self.name_to_pos.get(&(component, sensor, kind)) {
            return self.sensor_array[pos].id;
        }

        let this_id = self.next_id;
        self.next_id += 1;

        let sensor = Sensor {
            id: this_id,
            component: component.to_string(),
            sensor: sensor.to_string(),
            kind: kind.to_string()
        };
        self.add_sensor(sensor);

        self.save().unwrap();

        this_id
    }
}

fn open_database(database_path: &Path) -> Result<Database, Error> {
    if database_path.exists() {
        Database::open(database_path)
    } else {
        Database::create(Schema {
            dimensions: vec![
                Dimension { name: String::from("time"), chunk_size: 1000000 },
                Dimension { name: String::from("sensor_id"), chunk_size: 100 },
            ],
            values: vec![
                Value { name: String::from("value")}
            ]
        }, database_path)
    }
}

fn parse_time(s: &str) -> usize {
    let s = s.replace("a.m.", "am").replace("p.m.", "pm");
    let parsed = Utc.datetime_from_str(s.as_str(), "%d/%m/%Y %I:%M:%S %p").unwrap();
    parsed.timestamp_millis() as usize
}

fn parse_value(s: &str) -> usize {
    if s.is_empty() {
        return 0;
    }
    let num : f64 = s.parse::<f64>().unwrap();
    (num * 1000f64) as usize
}

fn load_reader<R: BufRead>(reader: &mut R, file_size: usize, sensors: &mut Sensors, txn: &mut Transaction) -> io::Result<()> {
    let mut bytes_read = 0;
    let mut last_pct = 0;
    let mut line_buffer = String::new();
    let mut last_time_str = String::new();
    let mut last_time_ms: usize = 0;
    for _line_num in 1.. {
        line_buffer.clear();
        let nr = reader.read_line(&mut line_buffer)?;
        if nr == 0 {
            break;
        }

        bytes_read += nr;

        let line = line_buffer.trim_end_matches('\n');

        let line = line.trim_start_matches('\0');

        //println!("line [{}]", line);

        let parts = line.split('\t').collect::<Vec<&str>>();
        let time_ms = if last_time_str.eq(parts[0]) {
            last_time_ms
        } else {
            last_time_str.clear();
            last_time_str.push_str(parts[0]);
            last_time_ms = parse_time(parts[0]);
            last_time_ms
        };
        let component = parts[1];
        let sensor = parts[2];
        let kind = parts[3];
        let value = parse_value(parts[4]);

        let sensor_id = sensors.get(component, sensor, kind);

        //println!("{} {} {}", time_ms, sensor_id, value);
        txn.add_row(&[time_ms, sensor_id, value]);

        let pct = bytes_read * 10 / file_size;
        if pct > last_pct {
            print!("{} ", pct*10);
            io::stdout().flush().unwrap();
            last_pct = pct;
        }
    }
    println!("Done");
    Ok(())
}

fn load_file(filename: &Path, sensors: &mut Sensors, txn: &mut Transaction) -> io::Result<()> {
    let file_size = std::fs::metadata(filename)?.len() as usize;
    let file = File::open(filename)?;

    if filename.to_str().unwrap().ends_with(".gz") {
        const COMPRESSION_RATIO : usize = 16;
        let mut gz_reader = flate2::read::GzDecoder::new(file);
        let mut reader = BufReader::new(gz_reader);
        load_reader(&mut reader, file_size * COMPRESSION_RATIO, sensors, txn);
    } else {
        let mut reader = BufReader::new(file);
        load_reader(&mut reader, file_size, sensors, txn);
    };

    Ok(())
}

fn load(sensors: &mut Sensors, matdb: &mut Database, filenames: &[PathBuf]) {
    for filename in filenames {
        println!("Loading {:?}", filename);

        /* Start a transaction */
        let mut txn = matdb.new_transaction().unwrap();

        /* Load this file */
        let now = Instant::now();
        load_file(filename.as_path(), sensors, &mut txn).unwrap();
        println!("Loaded and inserted in {:?}", now.elapsed());

        /* Save the transaction */
        let now = Instant::now();
        txn.commit().unwrap();
        println!("Saved in {:?}", now.elapsed());
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let mut sensors = Sensors::new();
    sensors.load().ok();

    /* Open the sensor-log database */
    let database_path = Path::new("sensor-log");
    let mut matdb = open_database(database_path).unwrap();

    let first_arg = &args[1];

    if first_arg == "load" {
        /* Load all the files (skip the first arg which is the program name) */
        let patterns = args.iter().skip(2).collect::<Vec<_>>();
        let mut filenames = Vec::new();
        for arg in patterns {
            filenames.extend(glob::glob(arg).unwrap().map(|s| s.unwrap()))
        }
        load(&mut sensors, &mut matdb, filenames.as_slice());
    } else if first_arg == "list" {
        /* List the database contents */
        let now = Instant::now();
        let txn = matdb.new_transaction().unwrap();
        let mut count = 0;
        for row in txn.query() {
            println!("{} {} {}", row[0], row[1], row[2]);
            count += 1;
        }
        txn.commit();
        println!("Queried {} rows in {:?}", count, now.elapsed());
    } else {
        panic!("Unknown command {}", first_arg);
    }
}
