FROM rust:1.48

WORKDIR /usr/src/ucz-dfs
COPY build.rs .
COPY Cargo.toml .
COPY docker/ ./docker/
COPY proto/ ./proto/
COPY src/ ./src/

RUN rustup component add rustfmt
RUN cargo install --path .

RUN chmod +x ./docker/start-namenode.sh
RUN chmod +x ./docker/start-datanode.sh

