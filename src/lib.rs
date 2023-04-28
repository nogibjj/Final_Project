use plotters::prelude::*;
use polars::prelude::*;
use std::env;
use aws_sdk_s3::Client;


pub async fn read_data(path:&str) -> DataFrame {
    // CsvReader::from_path(path).unwrap().finish().unwrap()
    let AWS_S3_BUCKET = env::var("AWS_S3_BUCKET").expect("AWS_S3_BUCKET must be set");
    let AWS_ACCESS_KEY_ID = env::var("AWS_ACCESS_KEY_ID");
    let AWS_SECRET_ACCESS_KEY = env::var("AWS_SECRET_ACCESS_KEY");
    let config = aws_config::from_env().region("us-east-1").load().await;
    let client = Client::new(&config);


    let result = client
       .get_object()
       .bucket(AWS_S3_BUCKET)
       .key(path)
       .send()
       .await
       .expect("Failed to get object");

    let bytes = result.body.collect().await.unwrap();
    let bytes = bytes.into_bytes();

    let cursor = std::io::Cursor::new(bytes);

    CsvReader::new(cursor).finish().unwrap()


}


pub fn plot(x: Vec<f64>, y: Vec<f64>, img_path:&str) {
    let data: Vec<(f64, f64)> = x.iter().cloned().zip(y.iter().cloned()).collect();
    let root_area = BitMapBackend::new(img_path, (700, 400)).into_drawing_area();
    root_area.fill(&WHITE).unwrap();

    // calculate min and max values for x and y
    let mut min_x = x[0];
    let mut max_x = x[0];
    let mut min_y = y[0];
    let mut max_y = y[0];
    for i in 0..x.len() {
        if x[i] < min_x {
            min_x = x[i];
        }
        if x[i] > max_x {
            max_x = x[i];
        }
        if y[i] < min_y {
            min_y = y[i];
        }
        if y[i] > max_y {
            max_y = y[i];
        }
    }

    let mut ctx = ChartBuilder::on(&root_area)
        .set_label_area_size(LabelAreaPosition::Left, 55)
        .set_label_area_size(LabelAreaPosition::Bottom, 55)
        .set_label_area_size(LabelAreaPosition::Right, 55)
        .set_label_area_size(LabelAreaPosition::Top, 55)
        .caption("Predictions", ("sans-serif", 45))
        .build_cartesian_2d(min_x..max_x, min_y..max_y)
        .unwrap();

    ctx.configure_mesh().draw().unwrap();

    // Draw Scatter Plot
    ctx.draw_series(data.iter().map(|point| Circle::new(*point, 4, BLUE)))
        .unwrap();
    root_area.present().unwrap();
    println!("Plot finished");
}


pub async fn plot_res(path:&str, img_path:&str){
    let df = read_data(path).await;

    let x_vec = df.column("xg").unwrap().f64().unwrap().to_vec();
    let mut x:Vec<f64> = Vec::new();
    for i in 0..x_vec.len() {
        x.push(x_vec[i].unwrap());
    }

    let y_vec = df.column("label_Goal").unwrap().i64().unwrap().to_vec();
    let mut y:Vec<f64> = Vec::new();
    for i in 0..y_vec.len() {
        y.push(y_vec[i].unwrap() as f64);
    }
    plot(x, y, img_path);
}
