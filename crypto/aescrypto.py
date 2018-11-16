
import os
import sys
import logging
import binascii
from datetime import datetime

#Import 3rd Party Packages:
from Crypto import Random
from Crypto.Cipher import AES

# Import Custom Modules - Currently scripts are not a complete package in the Git repo
from python_projects.system.platformutils import PlatformUtilsClass


STRING_TYPES = [str,unicode]

class AESCryptoClass:
    """AES requires a shared private key, which is used to encrypt and decrypt 
       data. AES is a block cipher so messages should be padded to block-size. 
       Valid key lengths are 16, 24, and 32.
       
       This class imports PyCrypto AES methods and uses 'AES.MODE_CBC' with a 
       random (see Crypto.Random)Initialization Vector of 16 bytes which is the
       block-size for this mode. 
       
       This class can be initialized with a private-key file-path or it's 
       methods can be called with a private-key input argument. The private-key 
       file can be refreshed by re-initializing with the '_reload' argument.
       
       Inputs:
       
           keyPath: str,unicode. Path to key file. File must be a Python module.
           
           secretKey: str/"""
    
    def __init__(self, keyPath, secretKey=None, _reload=False, logger=None):

        
        if secretKey == None:
            if keyPath != None:
                if os.path.exists(keyPath):
                    if os.path.isfile(keyPath):
                        ekeyDir, ekeyFile = PlatformUtilsClass.path_leaf(keyPath)
                        sys.path.insert(0,ekeyDir)
                        
                        if _reload:
                            reload(sys.modules[ekeyFile])
                            
                        exec('from %s import ekey'%ekeyFile)
                        self.ekey = ekey
                
                    else:
                        ekeyDir, ekeyFile = PlatformUtilsClass.path_leaf(keyPath,fileExten=True)
                        raise OSError("*keyPath: '%s' exists, but '%s' is not a file."%(ekeyDir,ekeyFile))
                else:
                    raise OSError("*keyPath: '%s' does not exists."%keyPath)
            
            else:
                raise Exception("Either input argument *key or *keyPath must be a valid input.")
                
        else:
            self.ekey = secretKey
            

    @staticmethod        
    def libraries():
        return ['os', 'binascii', 'Crypto.Random', 'Crypto.Cipher.AES']
    
    @classmethod
    def pad_message(self, message, blockSize):
        """Pad message with zeros by appending the zeros."""
        return message + b"\0" * (blockSize - len(message) % blockSize)
    
    @classmethod
    def cm_encrypt(self, message, secretKey, hexlify=True, encoding='utf-8',**kwds):
        """Provides AES encryption to the input message or string. This method is
           equivalent to 'encrypt' except that it is a class-method that does not
           require class initiation. See 'encrypt doc string for input information."""
        
        ekey = secretKey
            
        #AES.block_size is 16 bytes
        #Message must be multiples of the AES block-size, so message must be padded. The specified 
        #encoding is performed and then zeros are appended to end of message to meet block-size.
        message = self.pad_message(message.encode(encoding),AES.block_size)
        #Read bytes in block-sizes from cryptographic file object created from Random and create the 
        #Initialization Vector. A new iv needs to be created for each new message while using the same key.
        iv = Random.new().read(AES.block_size)
        #cipher object is initiated with key, mode, iv
        cipher = AES.new(ekey, AES.MODE_CBC, iv,**kwds)
        #Return encrypted byte string in hexidecimal format for readability and storage use cases.
        #The iv is sent along with the encrypted message and used to create the cipher object.
        if hexlify:
            return binascii.hexlify(iv + cipher.encrypt(message))
        else:
            return iv + cipher.encrypt(message)
    

    def encrypt(self, message, secretKey=None, hexlify=True, encoding='utf-8',**kwds):
        """Provides AES encryption for the input message or string type. See class
           doc string for more information on the encryption details.
           
           Inputs:
               message: str,unicode. Can be a ordinary, binary, hex, unicode string 
                                     type. Message or data to be encrypted.
                                     
               secretKey: str,unicode. Can be a ordinary, binary, hex, unicode string 
                                       type. Private key to be used to encrypt the 
                                       message.
                                       
               hexlify: boolean. Whether to return the ecrypted message in a 
                                 hexidecimal string representation.
                                 
               encoding: str,unicode. Encoding to encode message in before padding
                                      and encryption.
                                      
             **kwds: dict. Is used to pass key-word-arguments to any function/method
                             that is used within the current function/method. Also, 
                             used to pass in current method arguments. Once an argument
                             is used it is deleted from the dictionary and the continues
                             to pass the dictionary to the next level.
                                                                                             """
        
        #No key passed as argument so use class initiated key
        if secretKey == None:
            ekey = self.ekey
        else:
            ekey = secretKey
            
        #AES.block_size is 16 bytes
        AES.block_size = 16
        #Message must be multiples of the AES block-size, so message must be padded. The specified 
        #encoding is performed and then zeros are appended to end of message to meet block-size.
        message = self.pad_message(message.encode(encoding),AES.block_size)
        #Read bytes in block-sizes from cryptographic file object created from Random and create the 
        #Initialization Vector. A new iv needs to be created for each new message while using the same key.
        iv = Random.new().read(AES.block_size)
        #cipher object is initiated with key, mode, iv
        cipher = AES.new(ekey, AES.MODE_CBC, iv,**kwds)
        #Return encrypted byte string in hexidecimal format for readability and storage use cases.
        #The iv is sent along with the encrypted message and used to create the cipher object.
        if hexlify:
            return binascii.hexlify(iv + cipher.encrypt(message))
        else:
            return iv + cipher.encrypt(message)
        
    @classmethod 
    def cm_decrypt(self, ciphertext, secretKey, unhexlify=True, encoding='utf-8',**kwds):
        """Provides AES decryption to the input message or string. This method is
           equivalent to 'decrypt' except that it is a class-method that does not
           require class initiation. See 'decrypt doc string for input information."""
        
        ekey = secretKey
                
        #Get encrypted message in non-hexidecimal format or in byte-string format
        if unhexlify:
            ciphertext = binascii.unhexlify(ciphertext)
        #Extract iv from ciphertext. iv must be 16 btyes long
        iv = ciphertext[:AES.block_size]
        #Create new cipher object with key, mode, iv
        cipher = AES.new(ekey, AES.MODE_CBC, iv)
        #Slice message using block-size from ciphertext and decrypt
        plaintext = cipher.decrypt(ciphertext[AES.block_size:])
        #Strip off padded zeros from message and decode from specified encoding. 
        #This will return a unicode string type.
        return plaintext.rstrip(b"\0").decode(encoding)
        
    
    def decrypt(self, ciphertext, secretKey=None, unhexlify=True, encoding='utf-8',**kwds):
        """Provides AES decryption for the input message or string type. See class
           doc string for more information on the decryption details.
           
           Inputs:
               ciphertext: str,unicode. Can be a ordinary, binary, hex, unicode string 
                                        type. Message or data to be decrypted.
                                     
               secretKey: str,unicode. Can be a ordinary, binary, hex, unicode string 
                                       type. Private key to be used to decrypt the 
                                       message.
                                       
              unhexlify: boolean. Whether to return the decrypted message in a 
                                  non-hexidecimal string representation.
                                 
               encoding: str,unicode. Encoding to decode message in after decrypting 
                                      and de-padding the decrypted message.
                                      
             **kwds: dict. Is used to pass key-word-arguments to any function/method
                             that is used within the current function/method. Also, 
                             used to pass in current method arguments. Once an argument
                             is used it is deleted from the dictionary and the continues
                             to pass the dictionary to the next level.
                                                                                             """

        #No key passed as argument so use class initiated key
        if secretKey == None:
            ekey = self.ekey
        else:
            ekey = secretKey
                
        #Get encrypted message in non-hexidecimal format or in byte-string format
        if unhexlify:
            ciphertext = binascii.unhexlify(ciphertext)
        #Extract iv from ciphertext. iv must be 16 btyes long
        iv = ciphertext[:AES.block_size]
        #Create new cipher object with key, mode, iv
        cipher = AES.new(ekey, AES.MODE_CBC, iv)
        #Slice message using block-size from ciphertext and decrypt
        plaintext = cipher.decrypt(ciphertext[AES.block_size:])
        #Strip off padded zeros from message and decode from specified encoding. 
        #This will return a unicode string type.
        return plaintext.rstrip(b"\0").decode(encoding)
    
    
    @classmethod
    def cm_encrypt_file(self, filePath, secretKey, returnCiphertext=False,**kwds):        
        """AES file encryption. Encrypts file given by the file-path. File
           is written out with a '.enc' file-extension.
           
           Inputs:
               
               filepath: str/unicode type. Path to file including filename.
               
               key: str,unicode. Secret key. Defaults to None, which then uses
                                 the class initiation key that is imported from
                                 the key-file.
                                 
               hexlify: boolean. Whether to format the data in a hexidecimal format.
                                 This includes the data as a hexidecimal string and 
                                 the format of the data when written to a file.
                                 
             **kwds: dict. Is used to pass key-word-arguments to any function/method
                             that is used within the current function/method. Also, 
                             used to pass in current method arguments. Once an argument
                             is used it is deleted from the dictionary and the continues
                             to pass the dictionary to the next level.
               
                                                                                           """ 

        ekey = secretKey
                
        with open(filePath, 'rb') as rfile:
            plaintext = rfile.read()
        
        if secretKey == None:
            ciphertext = self.encrypt(plaintext,**kwds)
        else:
            ciphertext = self.encrypt(plaintext, secretKey=ekey,**kwds)
            
        with open(filePath + ".enc", 'wb') as wfile:
            wfile.write(ciphertext)
        
        if returnCiphertext:
            return ciphertext
        
        
    def encrypt_file(self, filePath, secretKey=None, returnCiphertext=False,**kwds):        
        """AES file encryption. Encrypts file given by the file-path. File
           is written out with a '.enc' file-extension.
           
           Inputs:
               
               filepath: str/unicode type. Path to file including filename.
               
               key: str,unicode. Secret key. Defaults to None, which then uses
                                 the class initiation key that is imported from
                                 the key-file.
                                 
               hexlify: boolean. Whether to format the data in a hexidecimal format.
                                 This includes the data as a hexidecimal string and 
                                 the format of the data when written to a file.
                                 
             **kwds: dict. Is used to pass key-word-arguments to any function/method
                             that is used within the current function/method. Also, 
                             used to pass in current method arguments. Once an argument
                             is used it is deleted from the dictionary and the continues
                             to pass the dictionary to the next level.
               
                                                                                           """ 
        
        if secretKey == None:
            ekey = self.ekey
        else:
            ekey = secretKey
                
        with open(filePath, 'rb') as rfile:
            plaintext = rfile.read()
        
        if secretKey == None:
            ciphertext = self.encrypt(plaintext,**kwds)
        else:
            ciphertext = self.encrypt(plaintext, secretKey=ekey,**kwds)
            
        with open(filePath + ".enc", 'wb') as wfile:
            wfile.write(ciphertext)
        
        if returnCiphertext:
            return ciphertext
        
        
    @classmethod
    def cm_decrypt_file(self, filePath, secretKey, returnPlaintext=False,**kwds):
        """AES file decryption. Decrypts file given by the file-path. File
           is written out with original name and extension.                  
           
           Inputs:
               
               filepath: str/unicode type. Path to file including filename.
               
               key: str/unicode. Secret key. Defaults to None, which then uses
                                 the class initiation key that is imported from
                                 the key-file.
                                 
               unhexlify: boolean. Whether to unformat the data from a hexidecimal 
                                   format. 
                                 
             **kwds: dict. Is used to pass key-word-arguments to any function/method
                             that is used within the current function/method. Also, 
                             used to pass in current method arguments. Once an argument
                             is used it is deleted from the dictionary and the continues
                             to pass the dictionary to the next level.                    
                                                                                             """
        
  
        ekey = secretKey
                
        with open(filePath, 'rb') as rfile:
            ciphertext = rfile.read()
            
        if secretKey == None:    
            plaintext = self.decrypt(ciphertext,**kwds)
        else:
            plaintext = self.decrypt(ciphertext, secretKey=ekey,**kwds)
            
        with open(filePath[:-4], 'wb') as wfile:
            wfile.write(plaintext)
         
        if returnPlaintext:
            return plaintext
        
        
    def decrypt_file(self, filePath, secretKey=None, returnPlaintext=False,**kwds):
        """AES file decryption. Decrypts file given by the file-path. File
           is written out with original name and extension.                  
           
           Inputs:
               
               filepath: str/unicode type. Path to file including filename.
               
               key: str/unicode. Secret key. Defaults to None, which then uses
                                 the class initiation key that is imported from
                                 the key-file.
                                 
               unhexlify: boolean. Whether to unformat the data from a hexidecimal 
                                   format. 
                                 
             **kwds: dict. Is used to pass key-word-arguments to any function/method
                             that is used within the current function/method. Also, 
                             used to pass in current method arguments. Once an argument
                             is used it is deleted from the dictionary and the continues
                             to pass the dictionary to the next level.                    
                                                                                             """
        
        if secretKey == None:
            ekey = self.ekey
        else:
            ekey = secretKey
                
        with open(filePath, 'rb') as rfile:
            ciphertext = rfile.read()
            
        if secretKey == None:    
            plaintext = self.decrypt(ciphertext,**kwds)
        else:
            plaintext = self.decrypt(ciphertext, secretKey=ekey,**kwds)
            
        with open(filePath[:-4], 'wb') as wfile:
            wfile.write(plaintext)
         
        if returnPlaintext:
            return plaintext

