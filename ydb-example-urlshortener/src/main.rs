use rocket::http::ContentType;
use rocket::response::Redirect;
use rocket::{form::Form, get, post, routes, uri, FromForm};
use rocket_dyn_templates::Template;
use std::collections::HashMap;

#[get("/")]
fn redirect() -> Redirect {
    Redirect::to(uri!(ui_index()))
}

#[get("/ui")]
fn ui_index() -> Template {
    let mut c = HashMap::<String, String>::new();
    c.insert("url".to_string(), "".to_string());
    return Template::render("test", &c);
}

#[derive(FromForm)]
struct AddUrlForm {
    url: String,
}

#[post("/ui", data = "<form>")]
fn ui_index_post(form: Form<AddUrlForm>) -> Template {
    let mut c = HashMap::<String, String>::new();
    c.insert("url".to_string(), form.url.clone());
    return Template::render("test", &c);
}

#[rocket::main]
async fn main() {
    rocket::build()
        .attach(Template::fairing())
        .mount("/", routes![redirect, ui_index, ui_index_post])
        .launch()
        .await;
}
