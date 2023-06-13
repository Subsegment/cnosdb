use std::io::Write;
use tskv::file_system::file::cursor::FileCursor;
use tskv::file_system::file_manager::create_file;
use tokio::runtime::Runtime;

use criterion::{criterion_group, criterion_main, Criterion};
use rand::Rng;


async fn run_iouring(id: u64, data: &[u8]) {
    let path = format!("/data1/syk/tmp0/test{}.txt",id);
    let mut file = FileCursor::from(create_file(path).await.unwrap());
    file.write(&data).await.unwrap();
}

fn test_iouring(c: &mut Criterion) {
    let data = rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(1024 * 1024 * 100)
        .map(u8::from)
        .collect::<Vec<u8>>();
    let mut id = 0;
    let rt = Runtime::new().unwrap();
    c.bench_function("io_uring", |b| {
        b.iter(|| {
            rt.block_on(run_iouring(id, &data));
            id += 1;
        })
    });
}

fn run_direct(id: u64, data: &[u8]) {
    let path = format!("/data1/syk/tmp1/test{}.txt",id);
    let mut file = std::fs::File::create(path).unwrap();
    file.write_all(&data).unwrap();
}

fn test_direct(c: &mut Criterion) {
    let data = rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(1024 * 1024 * 100)
        .map(u8::from)
        .collect::<Vec<u8>>();
    let mut id = 0;
    c.bench_function("direct", |b| {
        b.iter(|| {
            run_direct(id, &data);
            id += 1;
        })
    });
}

criterion_group!(benches, test_direct, test_iouring);
criterion_main!(benches);