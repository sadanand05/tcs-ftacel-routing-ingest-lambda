package com.foxtel.ingest;

import com.foxtel.ingest.aws.s3.S3Path;
import com.foxtel.ingest.exception.IngestException;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.BouncyGPG;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.callbacks.KeyringConfigCallbacks;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.InMemoryKeyring;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.KeyringConfig;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.KeyringConfigs;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;

/**
 * Reads encrypted customer data from S3 and computes deltas
 */
public class CustomerIngest
{
    /**
     * The current data file path
     */
    private final S3Path current;

    /**
     * Optional previous data file, if null this is a full load
     */
    private final S3Path previous;

    /**
     * The table name in DynamoDB
     */
    private final String tableName;

    /**
     * Private key file
     */
    private final String privateKeyFile;

    /**
     * Main program entry point
     * @param args the command line arguments
     * @throws IngestException thrown on failure to ingest
     */
    public static void main(String[] args) throws IngestException
    {
        Options options = new Options();

        options.addRequiredOption(null, "current", true, "Current ingest path");
        options.addOption(null, "previous", true, "Previous ingest path");
        options.addRequiredOption(null, "table", true, "Dynamo table name");
        options.addRequiredOption(null, "key", true, "Input private key");

        try
        {
            BouncyGPG.registerProvider();

            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse( options, args);

            S3Path previousPath = null;

            if (cmd.hasOption("previous"))
            {
                previousPath = new S3Path(cmd.getOptionValue("previous"));
            }

            CustomerIngest ingest = new CustomerIngest(new S3Path(cmd.getOptionValue("current")),
                    previousPath,
                    cmd.getOptionValue("table"),
                    cmd.getOptionValue("key"));

            ingest.process();
        }
        catch (IngestException e)
        {
            throw e;
        }
        catch (Throwable t)
        {
            throw new IngestException("Failed to process ingest", t);
        }
    }

    public CustomerIngest(S3Path current, S3Path previous, String tableName, String privateKeyFile)
    {
        this.current = current;
        this.previous = previous;
        this.tableName = tableName;
        this.privateKeyFile = privateKeyFile;
    }

    /**
     * Process the data files ingesting into DynamoDB
     */
    public void process() throws IngestException
    {
        try
        {
            long start = System.currentTimeMillis();

            KeyringConfig keyringConfig = getKeyringConfig(privateKeyFile, "F0xt3l@2021FTW");
            final InputStream cipherTextStream = new FileInputStream(new File("data/customer-001.csv.gpg"));

            final InputStream plaintextStream = BouncyGPG
                    .decryptAndVerifyStream()
                    .withConfig(keyringConfig)
                    .andIgnoreSignatures()
                    .fromEncryptedInputStream(cipherTextStream);



            String decrypted = IOUtils.toString(plaintextStream, "UTF-8");

            long end = System.currentTimeMillis();

            System.out.println("Decryption took: " + (end - start) + " millis");
            System.out.println(decrypted);
        }
        catch (IngestException e)
        {
            throw e;
        }
        catch (Throwable t)
        {
            throw new IngestException("Failed to decrypt file", t);
        }


    }

    /**
     * Loads the keyring for an exported private key
     * @param privateKeyFile the private key file
     * @param passphrase the passphase for the private key
     * @return the keyring config supplying the private key
     * @throws IngestException thrown on failure
     */
    private KeyringConfig getKeyringConfig(final String privateKeyFile, final String passphrase) throws IngestException
    {
        try
        {
            final InMemoryKeyring keyring = KeyringConfigs.forGpgExportedKeys(KeyringConfigCallbacks.withPassword(passphrase));
            final String privateKey = FileUtils.readFileToString(new File(privateKeyFile), "UTF-8");
            keyring.addSecretKey(privateKey.getBytes("US-ASCII"));
            return keyring;
        } catch (Throwable t)
        {
            throw new IngestException("Failed to load private key", t);
        }
    }
}
