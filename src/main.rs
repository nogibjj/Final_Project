extern crate actix_web;
use actix_web::{get, App, HttpResponse, HttpServer, Responder};


#[get("/")]
async fn welcome() -> impl Responder {
    HttpResponse::Ok().body("Hello!")
}

#[get("/artist/{artist_name}")]
async fn artist() -> impl Responder {
    HttpResponse::Ok().body("Other test!")
}


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    //add a print message to the console that the service is running
    println!("Starting service...");
    HttpServer::new(|| App::new().service(welcome).service(artist))
        .bind("0.0.0.0:8080")?
        .run()
        .await
}