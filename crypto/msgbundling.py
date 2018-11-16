# Author: Dustin Doubet
# Description:
# This is a utility class see class doc string

import hmac
import hashlib
import base64
import binascii

#Required for encryption:
from aescrypto import AESCryptoClass

class HMACMsgBundlingClass(AESCryptoClass):
    """Class for bundling and debundling string type data or messages. 
       Bundling messages involves encrypting the message (See AESCryptoClass)
       and then creating a digest (HMAC-hash) of the encrypted message and 
       then concatenating the digest and message together. 
       
       The digest and encrypted message are encoded in a hexidecimal 
       representative string. An optional encoding for the final bundled
       message is base64 encoding.
       
       You must initiate this class with the __init__ method and set the 
       configuration path or pass in a configuration dictionary.
       
       
       Class Methods:
       
           1) libraries
           2) errors
           3) required_config
           4) string_type
           5) make_digest
           6) msg_bundler
           7) msg_debundler
                                                                               
    """
    
    def __init__(self, configPath, configDict=None, logger_=None, encryption=True,
                                                             _reload=False, **kwds):
        """HMACMsgBundlingClass initiation. Initiates the instance of the class to
           an initial state.
           
           Inputs:
               
               configPath:   str,unicode. The path to the configuration file for this class.
                             The configuration file includes the HMAC and the 
                             encryption key file paths, message and HMAC 
                             positional info, digest and key sizes. See
                             default configuration file for more information.
                                        
               configDict:   dict. This is a dictionary with the same configuration parameters
                             as the configuration file, but in key,value form. All default
                             configuration parameters must be included. Please see class
                             method 'required_config()' for more information.
                                 
               encryption:   boolean. Whether to initiate the AESCryptoClass for encryption
                             and decryption of messages. Default is set to True.
                                   
              _reload:       boolean. Whether to apply a deep reload of the configuration file.
                             If you have made recent changes to the configuration file
                             and are creating a new instance of this class in the same
                             Python interpter then you will need to set argument to True.
                                
             **kwds:         dict. Is used to pass key-word-arguments to any function/method
                             that is used within the current function/method. Also, 
                             used to pass in current method arguments. Once an argument
                             is used it is deleted from the dictionary and the continues
                             to pass the dictionary to the next level.  
                                                                                                 
        """
                                               
        self.configDict = configDict
        self.encryption = encryption
        
        self.configKeyList = ['hkey','ekey','hkeyPath','ekeyPath','configPath',
                              'splitString','hashPosition','hashLength','msgPosition']
        
        self.requiredDict= {'hkeyPath':STRING_TYPES,'ekeyPath':STRING_TYPES,
                            'splitString':STRING_TYPES,'hashPosition':int,
                            'hashLength':int,'msgPosition':int}
        
        if encryption == False:
            del self.requiredDict['ekeyPath']
     
        if self.configDict == None:
            if configPath != None: 
                if os.path.exists(configPath):
                    if os.path.isfile(configPath):
                        self.configDir, self.configFile = PlatformUtilsClass.path_leaf(configPath)
                    else:
                        configDir, configFile = PlatformUtilsClass.path_leaf(
                                                             configPath, fileExten=True)
                        raise OSError("*configPath: '%s' exists, but '%s' is not a file."
                                      %(configPath,configFile))
                else:
                    raise OSError("*configPath: '%s' does not exists."%configPath)
            else:
                raise Exception("Either *configPath or *configDict must be "
                                "a valid input arguments.")
                                 
            try:
                sys.path.insert(0,self.configDir)
                
                if _reload:
                    reload(sys.modules[self.configFile])
                #TODO: Take out the exec statement and add a common import statement
                exec("""from %s import (ekeyPath,
                                        hkeyPath,
                                        splitString,
                                        hashPosition,
                                        hashLength,
                                        msgPosition)
                    """%self.configFile)

            except ImportError:
                raise Exception("Error while importing HMACMsgBundlingClass libraries "
                                "or objects from '%s' file."%self.configFile)
            except AttributeError:
                raise Exception("If HMACMsgBundlingClass is initiated with "
                                "*configDict = None then class variables *configFile "
                                "and *configDir must be created from a wrapper class.")
            else:
                self.configDict = {}
                self.configDict['ekeyPath']     = ekeyPath
                self.configDict['hkeyPath']     = hkeyPath
                self.configDict['splitString']  = splitString
                self.configDict['hashPosition'] = hashPosition
                self.configDict['hashLength']   = hashLength
                self.configDict['msgPosition']  = msgPosition    
        else:  
            for key in self.requiredDict.keys():
                if key not in self.configDict.keys():
                    raise KeyError("*configDict requires the following configuration "
                                   "parameter keys:"+str(self.requiredDict.keys()))
                
                if self.requiredDict[key] == STRING_TYPES:
                    if type(self.configDict[key]) not in STRING_TYPES:
                        raise TypeError("Incorrect type passed to *configDict['%s']. "
                                        "Required type is %s"%(key,key,type(key)))
                
                elif type(self.configDict[key]) != self.requiredDict[key]:
                    raise TypeError("Incorrect type passed to *configDict['%s']. "
                                    "Required type is %s"%(key,key,type(key)))
            
            for key in self.configDict.keys():
                if key not in self.configKeyList:
                    raise KeyError("*configDict parameter %s is not an exceptable "
                                   "configuration key."%key)
                    
                    
        if os.path.exists(self.configDict['hkeyPath']):
            if os.path.isfile(self.configDict['hkeyPath']):
                
                hkeyDir, hkeyFile = PlatformUtilsClass.path_leaf(
                                                self.configDict['hkeyPath'])
                sys.path.insert(0,hkeyDir)
                
                if _reload:
                    reload(sys.modules[hkeyFile])
                    
                exec('from %s import hkey'%hkeyFile)
                self.hkey = hkey
            else:
                hkeyDir, hkeyFile = PlatformUtilsClass.path_leaf(
                                                self.configDict['hkeyPath'], fileExten=True)
                raise OSError("*hkeyPath: '%s' exists, but '%s' is not a file."
                              %(hkeyDir,hkeyFile))
        else:
            raise OSError("*hkeyPath: '%s' does not exists."%hkeyPath)        

            
        if encryption:
                                     
        #----Initiate AES Encryption Class Using Configuration File Directory-------#
        
            AESCryptoClass.__init__(self, keyPath=self.configDict['ekeyPath'])
        
        #---------------------------------------------------------------------------#
        
        
    @classmethod
    def hmbc_libraries(self,encryption=True):
        libraries = ['os','sys','base64','hashlib','hmac']
        if encryption:
            for item in AESCryptoClass.libraries():
                if item not in libraries:
                    libraries.append(item)
            return libraries
        else:
            return libraries
        
    @staticmethod
    def hmbc_errors(encryption=True):
        errorList = ['ENCODING_ERROR','HASHING_ERROR','SPLITTING_ERROR',
                     'DECODING_ERROR','HASH_VERIFICATION_ERROR','MSG_NOT_TRUSTED']
        if encryption:
            errorList = errorList+['ENCRYPTION_ERROR','DECRYPTION_ERROR']
            return errorList
        else:
            return errorList
            
    @staticmethod       
    def required_config():
        print "\nconfigKeyList:  ['hkey','ekey','hkeyDir','ekeyDir','configDir',\
                                  'splitString','hashPosition','hashLength','additionalMessages',\
                                  'msgPosition','configDir']"
        
        print "\nrequiredDict= {'hkeyDir':str,'ekeyDir':str,'splitString':str,'hashPosition':int,\
                                'hashLength':int,'additionalMessages':int,'msgPosition':int}"

    @classmethod   
    def string_type(self,testObject):
        """Check for string type- str or unicode
           and return 'ordinary' for str, 'unicode'
           for unicode and 'NAS' for not a string"""
        
        if isinstance(testObject, str):
            return 'ordinary'
        elif isinstance(testObject, unicode):
            return 'unicode'
        else:
            return 'NaS'
        
    @classmethod
    def make_digest(self, message, key=None, hexdigest=True):
        """Return a HMAC SHA512 digest of the message."""
        import hmac
        import hashlib
        
        if key == None:
            hkey = self.hkey
        else:
            hkey = key
        
        if hexdigest:
            return hmac.new(hkey, message, hashlib.sha512).hexdigest()
        else:
            return hmac.new(hkey, message, hashlib.sha512).digest()
        
 

    def msg_bundler(self, message, key=None, b64Encode=True,**kwds):
        """Bundles messages by applying AES encryption, computing the HMAC 
           of the encrypted message and then concatinating the two together. The 
           HMAC and encrypted message are hex-strings (hexlified) for portability 
           while storing the bundled message. If chosen a base64 encoding is placed 
           on the bundled message. 
           
           This method can be used without initiating the class, but a key must be
           provided.
           
           
           Inputs:
           
               message:    str,unicode. The message to bundle
               
               key:        str,unicode. The secret key to use for the HMAC. If no
                           key is provided then the class initiation
                           key is used. This requires setting up a 
                           configuration file with a 'hkeyPath' variable.
                           See class initiation doc string.
               
               b64Encode:  boolean. Whether to base64 encode the bundled message.
                           Is used for readability and portablity between
                           systems.
                                   
             **kwds:       dict. Is used to pass key-word-arguments to any function/method
                           that is used within the current function/method. Also, 
                           used to pass in current method arguments. Once an argument
                           is used it is deleted from the dictionary and the continues
                           to pass the dictionary to the next level.
                             
                           Also, used in this method to attach additional messages to 
                           the message being bundled. The additional messages are 
                           appended to the input message and a configurable delimiter 
                           is used to split up the messages.
                                                                                           
        """
            
        #--------------#
        #  Function 1  #
        #--------------#
            
        def msg_test(_message,ignore=True):
            if type(_message) != list:
                _message = [_message]
                
            if len(_message) == 1:
                ignore = False
                
            for i,msg in enumerate(_message): 
                msgType = self.string_type(msg)

                if msgType == 'ordinary':
                    try:
                        uniMsg = unicode(msg,encoding='utf-8',errors='replace')
                    except:
                        print "Error while converting '%s' to unicode."%msg
                    else:
                        _message[i] = uniMsg

                elif msgType == 'NaS':
                    if ignore:
                        _message.remove(msg)
                        print "Removing message: %s"%msg#############
                    else:
                        raise TypeError("Incorrect type passed for message: %s. "
                                        "Required type is str/unicode."%msg)
            
            return _message
                 
        #----------------------------#
        #  Start msg_bundler Method  #
        #----------------------------#
        
        if key == None:
            hkey = self.hkey
        else:
            hkey = key        
        
        if kwds != {}:
            if 'extraMsgs' in kwds.keys():     
                if type(kwds['extraMsgs']) == list:
                    msgList = msg_test([message]+kwds['extraMsgs'])
                    #Create unicode object from list of secondary messages,
                    #using the configuration 'splitString' as the message seperator

                    combMessage = self.configDict['splitString'].join(msgList)
                    
                elif type(kwds['extraMsgs']) in STRING_TYPES:
                    msgList = msg_test([message,kwds['extraMsgs']])
                    
                    combMessage = self.configDict['splitString'].join(msgList)
                    
                else:
                    raise TypeError("Incorrect type passed to **kwds and key: 'extraMsgs'. "
                                    "Required type is str/unicode or list.")
                    
                #Clear key-value pair because **kwds is not being used as an argument 
                #and Python will continue to pass the key-value pair. This object references 
                #to a global object so the local is deleted.
                del kwds['extraMsgs']
            else:
                combMessage = msg_test(message)[0]
        else:
            combMessage = msg_test(message)[0]
            
            
        try:
            self.encryptMessage = self.encrypt(combMessage,**kwds)
        except:
            print "Error while encrypting message."
            return "ENCRYPTION_ERROR"
        
        try:
            self.digest  = self.make_digest(self.encryptMessage,key=hkey)
        except:
            print "Error while creating the message (HMAC) hash"
            return "HASHING_ERROR"
        
        try:
            if b64Encode:
                return base64.b64encode(self.digest+self.encryptMessage)
            else:
                return self.digest+self.encryptMessage
        
        except:
            print "Error while bundling message hash and ecrypted message"
            return "ENCODING_ERROR"
        
      
    def msg_debundler(self, msgBundle, key=None ,b64Decode=True, returnExtraMsgs=False, **kwds):
        """Debundles messages by stripping off and confirming the HMAC of the encrypted 
           message and then decrypting the (AES encrypted) message. If base64 encoding 
           applied to the bundled message then the bundled message is base64 decoded.
           
           This method can be used without initiating the class, but a key must be
           provided.
           
           
           Inputs:
           
               msgBundle:        str,unicode. The bundled message.
               
               key:              str,unicode. The secret key to use for the HMAC. If no
                                 key is provided then the class initiation
                                 key is used. This requires setting up a 
                                 configuration file with a 'hkeyPath' variable.
                                 See class initiation doc string.
               
               b64Encode:        boolean. Whether to base64 decode the bundled message.
                                 Is used for readability and portablity between
                                 systems.
                                   
               returnExtraMsgs:  boolean. Defaults to False. If set to True then 
                                 additional messages will be stripped off
                                 the original message and returned seperately.
                                   
             **kwds: dict.       Is used to pass key-word-arguments to any function/method
                                 that is used within the current function/method. Also, 
                                 used to pass in current method arguments. Once an argument
                                 is used it is deleted from the dictionary and the continues
                                 to pass the dictionary to the next level.
                                                                                               
        """

        if key == None:
            hkey = self.hkey
        else:
            hkey = key
         
            """try:
                self.configDict

            except NameError:
                config == False
                self.config_dict = {'hashLength':128,'splitString':'###','hashPosition': 0,
                                    'msgPosition': 1}##############

            else:
                config == True
            """   
        try:
            if b64Decode:
                msgBundle = base64.b64decode(msgBundle)
        except:
            return "DECODING_ERROR"
        
        try:
            if self.configDict['hashPosition']:
                self.receivedDigest = msgBundle[self.configDict['hashLength']:]
                self.receivedMessage = msgBundle[:self.configDict['hashLength']]
            else:
                self.receivedDigest = msgBundle[:self.configDict['hashLength']]
                self.receivedMessage = msgBundle[self.configDict['hashLength']:]
           
            """except AtributeError:
                self.receivedDigest = msgBundle[:self.configDict['hashLength']]####################
                self.receivedMessage = msgBundle[self.configDict['hashLength']:]
            """
        except:
            print "Error while debundling hash and encrypted message."
            return "SPLITTING_ERROR"
            
        try:
            self.digest = self.make_digest(self.receivedMessage,key=hkey)
            
        except:
            #remember to log that error could be from secondary message decryption or digest.
            print "Error while verifying the message (HMAC) hash."
            return "HASHING_ERROR"
                
        if self.digest == self.receivedDigest:

            try:
            	decryptMessage = self.decrypt(self.receivedMessage,**kwds)

            except:
                print "Error while decrypting message: %s"%self.receivedMessage
                return "DECRYPTION_ERROR"
            else:
                if decryptMessage in self.hmbc_errors(encryption=True):
                    return "MSG_NOT_TRUSTED"
            
            try:
                if returnExtraMsgs:
                    extraMsgs = decryptMessage.split(self.configDict['splitString'])
                    decryptMessage = extraMsgs[0]
                    extraMsgs.remove(decryptMessage)

                    return decryptMessage, extraMsgs
                else:
                    return decryptMessage

            except:###############
                #if class method is not initiated then self.configDict will not 
                #be available and will give error
                print "Error while splitting primary and secondary messages with "\
                      "split-string: %s."%self.configDict['splitString']
                return "SPLITTING_ERROR"
        else:
            print "Received hash does not match private hash. Message not trusted"
            return "HASH_VERIFICATION_ERROR"