
use derivative::Derivative;

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct Session {
    pub(crate) id: String,

    #[derivative(Debug = "ignore")]
    on_drop_callbacks: Vec<Box<dyn FnOnce(&mut Self) + Send>>,
}

impl Session {
    pub(crate) fn new(id: String) -> Self {
        return Self {
            id,
            on_drop_callbacks: Vec::new(),
        };
    }

    #[allow(dead_code)]
    pub(crate) fn on_drop(&mut self, f: Box<dyn FnOnce(&mut Self) + Send>) {
        self.on_drop_callbacks.push(f)
    }

    pub(crate) fn clone_without_ondrop(&self)->Self {
        return Self {
            id: self.id.clone(),
            on_drop_callbacks: Vec::new(),
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        println!("drop session: {}", &self.id);
        while let Some(on_drop) = self.on_drop_callbacks.pop() {
            on_drop(self)
        }
    }
}
