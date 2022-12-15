use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, Read, Seek, Write};
use std::path::Path;
use std::time::Instant;

use chrono::prelude::*;
use serde::{Serialize, Deserialize};

use matdb::{Datum, Dimension, Transaction, Value};

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

fn load_file(filename: &str, sensors: &mut Sensors, txn: &mut Transaction) -> io::Result<()> {
    let file_size = std::fs::metadata(filename)?.len();
    let file = File::open(filename)?;
    let mut reader = BufReader::new(file);

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

        let pct = reader.stream_position()? * 10 / file_size;
        if pct > last_pct {
            print!("{} ", pct*10);
            io::stdout().flush().unwrap();
            last_pct = pct;
        }
    }
    println!("Done");
    Ok(())
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let mut sensors = Sensors::new();
    sensors.load().ok();

    /* Make a database */
    let mut matdb = matdb::Database::create(matdb::Schema {
        dimensions: vec![
            Dimension { name: String::from("time"), chunk_size: 1000000 },
            Dimension { name: String::from("sensor_id"), chunk_size: 100 },
        ],
        values: vec![
            Value { name: String::from("value")}
        ]
    }, Path::new("sensordb")).unwrap();

    /* Start a transaction */
    let mut txn = matdb.new_transaction().unwrap();

    /* Load the file */
    let now = Instant::now();
    load_file(&args[1], &mut sensors, &mut txn).unwrap();
    println!("Loaded and inserted in {:?}", now.elapsed());

    /* Save the transaction */
    let now = Instant::now();
    txn.commit().unwrap();
    println!("Saved in {:?}", now.elapsed());

    /* Check the data is ok */
    let txn = matdb.new_transaction().unwrap();
    let now = Instant::now();
    //txn.load();
    println!("Reloaded in {:?}", now.elapsed());

    let mut count = 0;
    for _row in txn.query() {
        //println!("{} {} {}", row[0], row[1], row[2]);
        count += 1;
    }
    println!("Queried {} rows in {:?}", count, now.elapsed());
}
