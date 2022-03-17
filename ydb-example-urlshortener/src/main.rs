use rocket::routes;
use rocket_dyn_templates::Template;

mod ui;

#[rocket::main]
async fn main() {
    ui::build().launch().await;
}
