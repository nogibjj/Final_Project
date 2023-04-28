use plotters::prelude::*;
use polars::prelude::*;
use std::env;
use aws_sdk_s3::types::{
    CompressionType, CsvInput, CsvOutput, ExpressionType, FileHeaderInfo, InputSerialization,
    OutputSerialization, SelectObjectContentEventStream,
};
use aws_sdk_s3::Client;


pub async fn read_data(path:&str) -> DataFrame {
    // CsvReader::from_path(path).unwrap().finish().unwrap()
    let aws_s3_bucket: String = env::var("AWS_S3_BUCKET").expect("AWS_S3_BUCKET must be set");
    let config = aws_config::from_env().region("us-east-1").load().await;
    let client = Client::new(&config);


    let result = client
       .get_object()
       .bucket(aws_s3_bucket)
       .key(path)
       .send()
       .await
       .expect("Failed to get object");

    let bytes = result.body.collect().await.unwrap();
    let bytes = bytes.into_bytes();

    let cursor = std::io::Cursor::new(bytes);

    CsvReader::new(cursor).finish().unwrap()

}


async fn get_queried_bytes(query:&String, path:&str, header:FileHeaderInfo) -> Vec<u8>{
    let aws_s3_bucket = env::var("AWS_S3_BUCKET").expect("AWS_S3_BUCKET must be set");
    let config = aws_config::from_env().region("us-east-1").load().await;
    let client = Client::new(&config);

    let mut output = client
        .select_object_content()
        .bucket(aws_s3_bucket)
        .key(path)
        .expression_type(ExpressionType::Sql)
        .expression(query)
        .input_serialization(
            InputSerialization::builder()
                .csv(
                    CsvInput::builder()
                        .file_header_info(header)
                        .build(),
                )
                .compression_type(CompressionType::None)
                .build(),
        )
        .output_serialization(
            OutputSerialization::builder()
                .csv(CsvOutput::builder().build())
                .build(),
        )
        .send()
        .await
        .expect("Failed to select object content");


    // get the results of output and print them
    let mut results = Vec::new();


    while let Ok(Some(event)) = output.payload.recv().await {
        match event {
            SelectObjectContentEventStream::Records(records) => {
                let res = records
                .payload()
                .map(|p| std::str::from_utf8(p.as_ref()).unwrap())
                .unwrap_or("")
                .to_string()
                ;

                results.push(res);

            }
            SelectObjectContentEventStream::Stats(stats) => {
                println!("Stats: {:?}", stats.details().unwrap());
            }
            SelectObjectContentEventStream::Progress(progress) => {
                println!("Progress: {:?}", progress.details().unwrap());
            }
            SelectObjectContentEventStream::Cont(_) => {
                println!("Continuation Event");
            }
            SelectObjectContentEventStream::End(_) => {
                println!("End Event");
            }
            otherwise => panic!("Unknown event type: {:?}", otherwise),
        }
    }


    // convert results to bytes
    let mut bytes = Vec::<u8>::new();
    // convert results to bytes and store in test 
    for i in results {
        let temp = i.as_bytes().to_vec();
        bytes.extend(temp);
    }

    bytes
}


pub async fn get_queried_data(query:String, path:&str) -> DataFrame {
    let data_bytes = get_queried_bytes(&query, path, FileHeaderInfo::Use).await;
    let header_query: &str = "SELECT * FROM s3object s LIMIT 1";
    let header_bytes = get_queried_bytes(&header_query, path, FileHeaderInfo::None).await;

    // Concat header with data 
    let final_bytes = [header_bytes, data_bytes].concat();

    let cursor = std::io::Cursor::new(final_bytes);
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
