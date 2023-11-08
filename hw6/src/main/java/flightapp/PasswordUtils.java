package flightapp;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;


/**
 * A collection of utility methods to help with managing passwords
 */
public class PasswordUtils {
  /**
   * Generates a cryptographically-secure salted password.
   */
  public static byte[] saltAndHashPassword(String password) {
    byte[] salt = generateSalt();
    byte[] saltedHash = hashWithSalt(password, salt);
    // TODO: combine the salt and the salted hash into a single byte array that
    // can be written to the database
    byte[] saltAndHashed = new byte[SALT_LENGTH_BYTES + KEY_LENGTH_BYTES];
    for (int i = 0; i < SALT_LENGTH_BYTES; i++) {
      saltAndHashed[i] = salt[i];
    }
    for (int i = 0; i < KEY_LENGTH_BYTES; i++) {
      saltAndHashed[i + SALT_LENGTH_BYTES] = saltedHash[i];
    }
    return saltAndHashed;
  }

  /**
   * Verifies whether the plaintext password can be hashed to provided salted hashed password.
   */
  public static boolean plaintextMatchesSaltedHash(String plaintext, byte[] saltedHashed) {
    // TODO: extract the salt from the byte array (ie, undo the logic you implemented in 
    // saltAndHashPassword), then use it to check whether the user-provided plaintext
    // password matches the password hash.

    // Extract the salt
    byte[] salt = new byte[SALT_LENGTH_BYTES];
    for (int i = 0; i < SALT_LENGTH_BYTES; i++){
      salt[i] = saltedHashed[i];
    }

    // Reverse the logic and compare the salt
    byte[] newSaltedHash = hashWithSalt(plaintext, salt);
    for (int i = 0; i < SALT_LENGTH_BYTES; i++) {
      if (saltedHashed[i] != salt[i]){
        return false;
      }
    }
    // Compare the rest of the hash
    for (int i = 0; i < KEY_LENGTH_BYTES; i++) {
      if (saltedHashed[i + SALT_LENGTH_BYTES] != newSaltedHash[i]){
        return false;
      };
    }
    return true;
  }
  
  // Password hashing parameter constants.
  private static final int HASH_STRENGTH = 65536;
  private static final int KEY_LENGTH_BYTES = 128;
  private static final int SALT_LENGTH_BYTES = 16;

  /**
   * Generate a small bit of randomness to serve as a password "salt"
   */
  static byte[] generateSalt() {
    // TODO: implement this.
    byte[] salt = new byte[SALT_LENGTH_BYTES];
    Random random = new SecureRandom();
    random.nextBytes(salt);
    return salt;
  }

  /**
   * Uses the provided salt to generate a cryptographically-secure hash of the provided password.
   * The resultant byte array will be KEY_LENGTH_BYTES bytes long.
   */
  static byte[] hashWithSalt(String password, byte[] salt)
    throws IllegalStateException {
    // Specify the hash parameters, including the salt
    KeySpec spec = new PBEKeySpec(password.toCharArray(), salt,
                                  HASH_STRENGTH, KEY_LENGTH_BYTES * 8 /* length in bits */);

    // Hash the whole thing
    SecretKeyFactory factory = null;
    byte[] hash = null; 
    try {
      factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
      hash = factory.generateSecret(spec).getEncoded();
      return hash;
    } catch (NoSuchAlgorithmException | InvalidKeySpecException ex) {
      throw new IllegalStateException();
    }
  }

}
