use crate::error::{Result, UdfsError};

use prost::Message;

use tokio::io::{AsyncRead, AsyncReadExt};

pub async fn parse_message<T: Message + Default>(
    reader: &mut (impl AsyncRead + Unpin),
) -> Result<T> {
    let (size, _) = get_message_size(reader).await?;

    let mut buffer = vec![0u8; size as usize];
    reader.read_exact(buffer.as_mut()).await?;

    let message = T::decode(buffer.as_ref())?;
    Ok(message)
}

async fn get_message_size(reader: &mut (impl AsyncRead + Unpin)) -> Result<(u64, u8)> {
    let mut result = 0;
    let mut shift = 0;
    for bytes_read in 1..=10 {
        let tmp = reader.read_u8().await?;
        result |= tmp as u64 & 0x7f << shift;
        if tmp < 0x80 {
            return Ok((result, bytes_read));
        }
        shift += 7;
    }

    Err(UdfsError::ProtoError("invalid varint".to_owned()))
}

#[cfg(test)]
mod test {

    use super::parse_message;

    use crate::proto::Operation;

    use bytes::Bytes;
    use prost::Message;
    use tokio::io::Result;
    use tokio_util::io::StreamReader;

    #[tokio::test]
    async fn buffer_with_multiple_messages() {
        let mut buffer = vec![];

        let first_op = Operation { op: 0 };
        first_op
            .encode_length_delimited(&mut buffer)
            .expect("Should encode");

        let second_op = Operation { op: 1 };
        second_op
            .encode_length_delimited(&mut buffer)
            .expect("Should encode");

        let third_op = Operation { op: 0 };
        third_op
            .encode_length_delimited(&mut buffer)
            .expect("Should encode");

        let stream = tokio_stream::iter(vec![Result::Ok(Bytes::from(buffer))]);
        let mut buffer = StreamReader::new(stream);

        let message: Operation = parse_message(&mut buffer).await.expect("Should work fine");
        assert_eq!(message, first_op);

        let message: Operation = parse_message(&mut buffer).await.expect("Should work fine");
        assert_eq!(message, second_op);

        let message: Operation = parse_message(&mut buffer).await.expect("Should work fine");
        assert_eq!(message, third_op);
    }
}
