from Crypto.Cipher import AES
import binascii

class Crypt:
    #解密
    def decrypt(data, key):
        """aes解密
        :param key:
        :param data:
        """
        cipher = AES.new(key, AES.MODE_ECB)
        result = binascii.a2b_hex(data)  # 十六进制还原成二进制
        decrypted = cipher.decrypt(result)
        return decrypted.rstrip(b'\r\x06\x05\x07\x10\x02\x03\x04\x08\x09\x01').decode('utf-8')  # 解密完成后将加密时添加的多余字符'\0'删除


    def encrypt(text,key):
        cryptor = AES.new(key, AES.MODE_ECB)
        # 这里密钥key 长度必须为16（AES-128）、24（AES-192）、或32（AES-256）Bytes 长度.目前AES-128足够用
        length = 16
        count = len(text)
        if (count % length != 0):
            add = length - (count % length)
        else:
            add = 0
        text = text + ('\x10' * add)
        ciphertext = cryptor.encrypt(text)
            # 因为AES加密时候得到的字符串不一定是ascii字符集的，输出到终端或者保存时候可能存在问题
            # 所以这里统一把加密后的字符串转化为16进制字符串
        return binascii.b2a_hex(ciphertext)
