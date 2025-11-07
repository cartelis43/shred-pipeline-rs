// This file contains integration tests for the Receiver and Decoder layers of the Shred Pipeline project.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::receiver::Receiver;
    use crate::decoder::Decoder;

    #[test]
    fn test_receiver_initialization() {
        let receiver = Receiver::new();
        assert!(receiver.is_initialized());
    }

    #[test]
    fn test_receiver_receive_data() {
        let mut receiver = Receiver::new();
        let data = vec![1, 2, 3, 4];
        assert!(receiver.receive(data).is_ok());
    }

    #[test]
    fn test_decoder_initialization() {
        let decoder = Decoder::new();
        assert!(decoder.is_initialized());
    }

    #[test]
    fn test_decoder_decode_data() {
        let decoder = Decoder::new();
        let encoded_data = vec![1, 2, 3, 4]; // Example encoded data
        let result = decoder.decode(&encoded_data);
        assert!(result.is_ok());
    }

    #[test]
    fn test_integration_receiver_decoder() {
        let mut receiver = Receiver::new();
        let decoder = Decoder::new();
        
        let data = vec![1, 2, 3, 4];
        assert!(receiver.receive(data).is_ok());
        
        let received_data = receiver.get_received_data();
        let result = decoder.decode(&received_data);
        assert!(result.is_ok());
    }
}