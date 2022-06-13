# Persistent Data Container

<a href="https://docs.rs/crio"><img src="https://docs.rs/crio/badge.svg"></a>

An easy to use persistent data storage library. Integrates well with Serde.

Version 2 of this crate is incompatible with files created with version 1 of this crate.

## Usage

```rust
use crio::Client;
use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Message {
    id: usize,
    message: String,
}

let msg1 = Message {
    id: 1,
    message: "Hello there, you suck".to_string(),
};
let msg2 = Message {
    id: 2,
    message: "No you".to_string(),
};
let msg3 = Message {
    id: 3,
    message: "You both suck".to_string(),
};
let messages = vec![msg1, msg2, msg3];
let client: Client<Message> = Client::new("messages", false)?; // If no file is found, a new empty file is created.
client.write_many(&messages)?; // If no file is found, a new file is created and then written to. Append is set to false such that it overwrites any previous value stored on the same file
let returned_messages = client.load()?;
if let Some(data) = returned_messages {
    assert_eq!(messages, data);
} else {
    panic!("File is empty");
}
```
