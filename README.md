# Persistent Data Container

An easy to use persistent data storage library. Integrates well with Serde.

## Usage

```rust
#[derive(Serialize, Deserialize, Debug)]
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
let path = PathBuf::new("messages.crf");
let client: Client<Message> = Client::new(path)?; // If no file is found, a new empty file is created
client.write(messages)?; // If no file is found, a new file is created and then written to
let messages = client.load()?;
if let Some(data) = messages {
    println!("Here are your messages: {:?}", data);
} else {
    panic!("File is empty");
}
```
