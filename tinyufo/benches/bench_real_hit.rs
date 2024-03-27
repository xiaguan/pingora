use std::error::Error;
use std::fs::File;
use std::num::NonZeroUsize;
use std::sync::Arc;

use csv::ReaderBuilder;

fn bench_lru(keys: Arc<Vec<String>>, cache_size: usize) -> usize {
    let mut lru = lru::LruCache::<String, ()>::new(NonZeroUsize::new(cache_size).unwrap());
    let mut hit = 0;

    for key in keys.as_ref() {
        if lru.get(key).is_some() {
            hit += 1;
        } else {
            lru.put(key.clone(), ());
        }
    }

    hit
}

fn bench_moka(keys: Arc<Vec<String>>, cache_size: usize) -> usize {
    let moka = moka::sync::Cache::new(cache_size as u64);
    let mut hit = 0;

    for key in keys.as_ref() {
        if moka.get(key).is_some() {
            hit += 1;
        } else {
            moka.insert(key.clone(), ());
        }
    }

    hit
}

fn bench_quick_cache(keys: Arc<Vec<String>>, cache_size: usize) -> usize {
    let quick_cache = quick_cache::sync::Cache::new(cache_size);
    let mut hit = 0;

    for key in keys.as_ref() {
        if quick_cache.get(key).is_some() {
            hit += 1;
        } else {
            quick_cache.insert(key.clone(), ());
        }
    }

    hit
}

fn bench_tinyufo(keys: &Vec<String>, cache_size: usize) -> usize {
    let tinyufo = tinyufo::TinyUfo::new(cache_size, cache_size);
    let mut hit = 0;

    for key in keys {
        if tinyufo.get(key).is_some() {
            hit += 1;
        } else {
            tinyufo.put(key.clone(), (), 1);
        }
    }

    hit
}

fn bench(keys: &Arc<Vec<String>>, cache_size: usize) {
    // Use multi threading to run the benchmarks
    let keys = Arc::clone(keys);

    let lru_handle = std::thread::spawn({
        let keys = Arc::clone(&keys);
        move || bench_lru(keys, cache_size)
    });

    let moka_handle = std::thread::spawn({
        let keys = Arc::clone(&keys);
        move || bench_moka(keys, cache_size)
    });

    let quick_cache_handle = std::thread::spawn({
        let keys = Arc::clone(&keys);
        move || bench_quick_cache(keys, cache_size)
    });

    let tinyufo_handle = std::thread::spawn({
        let keys = Arc::clone(&keys);
        move || bench_tinyufo(&keys, cache_size)
    });

    let lru_hit = lru_handle.join().unwrap();
    let moka_hit = moka_handle.join().unwrap();
    let quick_cache_hit = quick_cache_handle.join().unwrap();
    let tinyufo_hit = tinyufo_handle.join().unwrap();

    let iterations = keys.len();

    print!("{:.2}%\t\t", lru_hit as f32 / iterations as f32 * 100.0);
    print!("{:.2}%\t\t", moka_hit as f32 / iterations as f32 * 100.0);
    print!(
        "{:.2}%\t\t",
        quick_cache_hit as f32 / iterations as f32 * 100.0
    );
    println!("{:.2}%", tinyufo_hit as f32 / iterations as f32 * 100.0);
}

const TRACE_SIZE: usize = 5_000_000;

fn main() -> Result<(), Box<dyn Error>> {
    let mut first_column: Vec<String> = Vec::new();
    let file = File::open("/home/susun/datasets/photo_big.csv")?;

    let mut reader = ReaderBuilder::new()
        .delimiter(b' ') // 设置空格为分隔符
        .from_reader(file);

    let mut count = 0;

    for result in reader.records() {
        let record = result?;
        first_column.push(record[1].to_string());
        if count == TRACE_SIZE {
            break;
        }
        count += 1;
    }

    // Change first_column to Arc<Vec<String>> to share it between threads
    let first_column = Arc::new(first_column);

    println!("Read {:} records", first_column.len());
    println!("cache size\tlru\t\tmoka\t\tQuickC\t\tTinyUFO",);

    for cache_capacity in [0.005, 0.01, 0.05, 0.1, 0.25] {
        let cache_size = (cache_capacity * first_column.len() as f32).round() as usize;
        print!("{:.4}\t\t", cache_capacity);
        bench(&first_column, cache_size);
    }

    Ok(())
}
