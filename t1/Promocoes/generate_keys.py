from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256

class KeyManager:

    def __init__(self):
        self.my_key = None
        pass

    def generate_key(self, publisher_name):
        key = RSA.generate(4096)

        public_key_pem = key.publickey().export_key()

        with open(f"{publisher_name}.pem", "wb") as f:
            f.write(public_key_pem)

        self.private_key = key

    def sign(self, message):
        h = SHA256.new(message)
        signature = pkcs1_15.new(self.private_key).sign(h)
        return signature

    def check_signature(self, message, signature, publisher_name):
        try:
            key = RSA.import_key(open(f'{publisher_name}.der').read())
        except:
            print("Could not load key")
            raise
        h = SHA256.new(message)
        try:
            pkcs1_15.new(key).verify(h, signature)
            return True
        except (ValueError, TypeError):
            print("Error checking the signature")
            return False

if __name__ == "__main__":
    message = b"lalala"

    key = RSA.generate(4096)

    # Só é útil para salvar os arquivos
    private_key_pem = key.export_key()
    public_key_pem = key.publickey().export_key()

    h = SHA256.new(message)
    signature = pkcs1_15.new(key).sign(h)

    key = key.publickey()
    h = SHA256.new(message)
    try:
        pkcs1_15.new(key).verify(h, signature)
        print("Worked")
    except (ValueError, TypeError):
        print("Error checking the signature")