use clap::Parser;
use data_encoding::HEXLOWER;
use iroh::SecretKey;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long, default_value_t = 1)]
    count: usize,
}

fn main() {
    let cli = Cli::parse();

    for index in 0..cli.count {
        let secret = SecretKey::generate();
        let endpoint_id = secret.public();
        let secret_hex = HEXLOWER.encode(&secret.to_bytes());

        println!("secret={secret_hex}");
        println!("endpoint={endpoint_id}");

        if index + 1 < cli.count {
            println!();
        }
    }
}
