use actix_web::{HttpResponse, HttpRequest, Responder};

pub struct Index;
impl actix_web::dev::HttpServiceFactory for Index {
    fn register(self, __config: &mut actix_web::dev::AppService) {
        async fn index(_req: HttpRequest) -> impl Responder {
            HttpResponse::Ok().body("Welcome to the Premier Soccer Analyzer!")
        }
        let __resource = actix_web::Resource::new("/")
            .name("index")
            .guard(actix_web::guard::Get())
            .to(index);
        actix_web::dev::HttpServiceFactory::register(__resource, __config)
    }
}