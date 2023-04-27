extern crate actix_web;
use actix_web::{get, App, HttpResponse, HttpServer, Responder};
// use aws_sdk_s3::types::ByteStream;
//use aws_sdk_s3::Client;
//use std::env;
//use serde_json::value::Value;
use std::fs;

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Welcome to the Premier Soccer Analyzer!")
}

//function that plots the team results
#[get("/team_results")]
async fn team_result_plot() -> Result<HttpResponse, actix_web::error::ParseError> {
    const TEAM_RES: &str = "temp_data/model_res_teams.csv";
    const TEAM_RES_PNG: &str = "team_res.png";
    final_project::plot_res(TEAM_RES,TEAM_RES_PNG);

    // run the plot function and show plot on the actix server
    let image_data = fs::read(TEAM_RES_PNG)?;
    Ok(HttpResponse::Ok()
        .content_type("image/png")
        .body(image_data))
}

//function that plots the player results
#[get("/player_results")]
async fn player_result_plot() -> Result<HttpResponse, actix_web::error::ParseError> {
    const PLAY_RES: &str = "temp_data/model_res_players.csv";
    const PLAY_RES_PNG: &str = "player_res.png";
    final_project::plot_res(PLAY_RES,PLAY_RES_PNG);

    // run the plot function and show plot on the actix server
    let image_data = fs::read(PLAY_RES_PNG)?;
    Ok(HttpResponse::Ok()
        .content_type("image/png")
        .body(image_data))
}


// #[get("/team/{team_name}")]
// async fn team() -> impl Responder {
//     HttpResponse::Ok().body("Hello!")
// }

// #[get("/player/{player_name}")]
// async fn player() -> impl Responder {
//     HttpResponse::Ok().body("Other test!")
// }


// #[actix_web::main]
// async fn main() -> std::io::Result<()> {
//     //add a print message to the console that the service is running
//     println!("Starting service...");
//     HttpServer::new(|| App::new().service(team).service(player))
//         .bind("0.0.0.0:8080")?
//         .run()
//         .await
// }


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    //let AWS_S3_BUCKET = env::var("AWS_S3_BUCKET").expect("AWS_S3_BUCKET must be set");
    // let AWS_ACCESS_KEY_ID = env::var("AWS_ACCESS_KEY_ID")
    // let AWS_SECRET_ACCESS_KEY = env::var("AWS_SECRET_ACCESS_KEY")
    // let config = aws_config::load_from_env().await;
    //let config = aws_config::from_env().region("us-east-1").load().await;
    //let client = Client::new(&config);



    //let result = client
    //    .get_object()
    //    .bucket(AWS_S3_BUCKET)
    //    .key("raw-data/tags2name.csv")
    //    .send()
    //    .await
    //    .expect("Failed to get object");

    // let bytes = result.body.collect().await?.into_bytes();
    // let thing: Thing = serde_json::from_slice(&bytes).unwrap();

    //let res = result
     //   .unwrap()
     //   .json::<HashMap<String, Value>>()
     //   .await;

    //print!("{:?}", res.body)

    HttpServer::new(|| {
        App::new()
            .service(hello)
            .service(team_result_plot)
            .service(player_result_plot)
    })
    .bind("0.0.0.0:8080")?
    .run()
    .await

}


