
use derivative::Derivative;

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct Session {
    pub(crate) id: String,

    #[derivative(Debug = "ignore")]
    on_drop_callbacks: Vec<Box<dyn FnOnce() + Send>>,
}

impl Session {
    pub(crate) fn new(id: String) -> Self {
        return Self {
            id,
            on_drop_callbacks: Vec::new(),
        };
    }

    #[allow(dead_code)]
    pub(crate) fn on_drop(&mut self, f: Box<dyn FnOnce() + Send>) {
        self.on_drop_callbacks.push(f)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        println!("drop session: {}", &self.id);
        while let Some(on_drop) = self.on_drop_callbacks.pop() {
            on_drop()
        }
    }
}
