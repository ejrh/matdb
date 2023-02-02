use std::cmp::max;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

use chrono::prelude::*;
use serde::{Serialize, Deserialize};

use matdb::{Dimension, Value, Schema, Transaction, Database, Error, Datum};
use matdb::Error::DataError;

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
            // UNSAFE - sensor is now owned by the Sensors struct, so we can
            // extend the lifetime of references to its String fields to the
            // lifetime of the struct.
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

fn parse_time(s: &str) -> Result<usize, Error> {
    let s = s.replace("a.m.", "am").replace("p.m.", "pm");
    let parsed = Utc.datetime_from_str(s.as_str(), "%d/%m/%Y %I:%M:%S %p")
        .map_err(|_e| DataError)?;
    Ok(parsed.timestamp_millis() as usize)
}

fn parse_value(s: &str) -> Result<usize, Error> {
    if s.is_empty() {
        return Ok(0);
    }
    let num : f64 = s.parse::<f64>().map_err(|_e| DataError)?;
    Ok((num * 1000f64) as usize)
}

struct Item {
    time_ms: Datum,
    sensor_id: Datum,
    value: Datum
}

fn parse_line(
    line: &str,
    sensors_shared: &Arc<Mutex<&mut Sensors>>,
    last_time_str: &mut String,
    last_time_ms: &mut usize
) -> Result<Item, Error> {
    let parts = line.split('\t').collect::<Vec<&str>>();
    if parts.len() != 5 {
        return Err(DataError)
    }

    let time_ms: Datum = if last_time_str.as_str().eq(parts[0]) {
        *last_time_ms
    } else {
        let time_ms = parse_time(parts[0])?;
        last_time_str.clear();
        last_time_str.push_str(parts[0]);
        *last_time_ms = time_ms;
        time_ms
    };
    let component = parts[1];
    let sensor = parts[2];
    let kind = parts[3];
    let value = parse_value(parts[4])?;

    let sensor_id = {
        let mut guard = sensors_shared.lock().unwrap();
        (*guard).get(component, sensor, kind)
    };

    Ok(Item { time_ms, sensor_id, value })
}

fn parse_reader<R: BufRead>(reader: &mut R, file_size: usize, sensors_shared: &Arc<Mutex<&mut Sensors>>) -> Result<Vec<Item>, Error> {
    let mut bytes_read = 0;
    let mut last_pct = 0;
    let mut line_buffer = String::new();
    let mut last_time_str = String::new();
    let mut last_time_ms: usize = 0;
    let mut items = Vec::new();

    for line_num in 1.. {
        line_buffer.clear();
        let nr = reader.read_line(&mut line_buffer)?;
        if nr == 0 {
            break;
        }

        bytes_read += nr;

        let line = line_buffer.trim_end_matches('\n');

        let line = line.trim_start_matches('\0');

        //println!("line [{}]", line);

        let item_res = parse_line(line, sensors_shared, &mut last_time_str, &mut last_time_ms);
        if let Ok(item) = item_res {
            items.push(item);
        } else {
            println!("Skipping unparsable line {line_num}: {line}");
        }

        //println!("{} {} {}", time_ms, sensor_id, value);

        let pct = bytes_read * 10 / file_size;
        if pct > last_pct {
            //print!("{} ", pct*10);
            io::stdout().flush().unwrap();
            last_pct = pct;
        }
    }
    //println!("Done ({} rows)", items.len());
    Ok(items)
}

fn parse_file(filename: &Path, sensors_shared: &Arc<Mutex<&mut Sensors>>) -> Result<Vec<Item>, Error> {
    let file_size = std::fs::metadata(filename)?.len() as usize;
    let file = File::open(filename)?;

    if filename.to_str().unwrap().ends_with(".gz") {
        const COMPRESSION_RATIO : usize = 16;
        let gz_reader = flate2::read::GzDecoder::new(file);
        let mut reader = BufReader::new(gz_reader);
        parse_reader(&mut reader, file_size * COMPRESSION_RATIO, sensors_shared)
    } else {
        let mut reader = BufReader::new(file);
        parse_reader(&mut reader, file_size, sensors_shared)
    }
}

fn load_data(items: &Vec<Item>, txn: &mut Transaction) {
    for item in items {
        txn.add_row(&[item.time_ms, item.sensor_id, item.value]);
    }
}

fn load(sensors: &mut Sensors, matdb: &mut Database, filenames: &[PathBuf]) {
    let num_parser_threads = max(1, thread::available_parallelism().map(|x| x.get()).unwrap_or(1) - 1);

    let (sender, receiver) = channel();

    let sensors_shared = &Arc::new(Mutex::new(sensors));

    let chunk_size = max(1, (filenames.len() - 1) / num_parser_threads + 1);
    assert!(chunk_size * num_parser_threads >= filenames.len());

    println!("Loading {} files using {} parser threads", filenames.len(), num_parser_threads);

    thread::scope(|s| {
        for (i, chunk) in filenames.chunks(chunk_size).enumerate() {
            let sender = sender.clone();
            let sensors_shared = sensors_shared.clone();

            thread::Builder::new()
                .name(format!("Worker {i}"))
                .spawn_scoped(s, move || {
                    for filename in chunk {
                        let now = Instant::now();
                        let items = parse_file(filename.as_path(), &sensors_shared).unwrap();
                        let parse_ms = now.elapsed();
                        sender.send((filename, parse_ms, items)).unwrap();
                    }
                }).unwrap();
        }

        drop(sender);

        let mut item_count = 0;

        for (filename, parse_ms, items) in receiver {
            println!("Parsed {filename:?} in {parse_ms:?}");

            /* Start a transaction */
            let mut txn = matdb.new_transaction().unwrap();

            /* Insert the data */
            let now = Instant::now();
            load_data(&items, &mut txn);
            println!("Inserted in {:?}", now.elapsed());
            item_count += items.len();

            /* Save the transaction */
            let now = Instant::now();
            txn.commit().unwrap();
            println!("Saved in {:?}", now.elapsed());
        }

        println!("Inserted a total of {item_count} items");
    });
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
        load(&mut sensors, &mut matdb, &filenames);
    } else if first_arg == "list" {
        /* List the database contents */
        let now = Instant::now();
        let txn = matdb.new_transaction().unwrap();
        let mut count = 0;
        for row in txn.query() {
            println!("{} {} {}", row[0], row[1], row[2]);
            count += 1;
        }
        txn.commit().unwrap();
        println!("Queried {} rows in {:?}", count, now.elapsed());
    } else {
        panic!("Unknown command {first_arg}");
    }
}
