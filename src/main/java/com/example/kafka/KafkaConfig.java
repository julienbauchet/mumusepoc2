package com.example.kafka;

import static java.lang.String.format;
import static java.lang.System.getenv;

import java.io.File;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {

	 @Value("${topic}")
  private String topic;

	 @Value("${consumer.group}")
  private String consumerGroup;

  public Properties getProperties() {
    return buildDefaults();
  }

  private Properties buildDefaults() {
	  
	  
	  
    Properties properties = new Properties();
    List<String> hostPorts = new ArrayList<String>();

    for (String url : Stream.of(getenv("KAFKA_URL").split(","))
    	      .map (elem -> new String(elem))
    	      .collect(Collectors.toList())) {
      try {
        URI uri = new URI(url);
        hostPorts.add(format("%s:%d", uri.getHost(), uri.getPort()));

        switch (uri.getScheme()) {
          case "kafka":
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
            break;
          case "kafka+ssl":
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");

            try {
              EnvKeyStore envTrustStore;//= EnvKeyStore.createWithRandomPassword("KAFKA_TRUSTED_CERT");
              
              envTrustStore = new EnvKeyStore("-----BEGIN CERTIFICATE-----\n" + 
              		"MIIDfzCCAmegAwIBAgIBADANBgkqhkiG9w0BAQsFADAyMTAwLgYDVQQDDCdjYS02\n" + 
              		"NjhjNDhmZC04OTg1LTQwZTAtYjM1Ny0wYTdhNTNhYWRmMTEwHhcNMTcwOTA1MTI1\n" + 
              		"OTEwWhcNMjcwOTA1MTI1OTEwWjAyMTAwLgYDVQQDDCdjYS02NjhjNDhmZC04OTg1\n" + 
              		"LTQwZTAtYjM1Ny0wYTdhNTNhYWRmMTEwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAw\n" + 
              		"ggEKAoIBAQChoQWLp0qQzxrDa95thm2l9URpnECq7Ea/jqis1V9UGPJegt90ieqG\n" + 
              		"E4B8BMiMFSMT+xPBzpcBOgPv92j3tn4TbkAk630CnVAf2uHlWmZXEiTKEqgISjaa\n" + 
              		"67sRRmnG86HI3pY353Qyf3MnF8sP06mWZxJosugXfreiVKIGPfSJkfYZZBf7eApC\n" + 
              		"uhoF/OFOxzaFdCiEsYVCYh3i3Mpa6/xhN8hMyDoo7iPL4SGdfwDPne9g7wmRYtry\n" + 
              		"j+KM4XF4SuvBJ7Xx5DL32z3cs/DK/4JICSaP2xRguOiRGImbKra/xwMPcQDetjgm\n" + 
              		"Y/Zc/ya/RJDdgYaGU6pyq0HCcichOzxjAgMBAAGjgZ8wgZwwHQYDVR0OBBYEFL8u\n" + 
              		"ckUlrhsu60BJ33LrnD+XWvuVMA8GA1UdEwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQD\n" + 
              		"AgG2MFoGA1UdIwRTMFGAFL8uckUlrhsu60BJ33LrnD+XWvuVoTakNDAyMTAwLgYD\n" + 
              		"VQQDDCdjYS02NjhjNDhmZC04OTg1LTQwZTAtYjM1Ny0wYTdhNTNhYWRmMTGCAQAw\n" + 
              		"DQYJKoZIhvcNAQELBQADggEBACKtbiShWPONObbcTwygFeC2ebpLNC3KAKhmeaP1\n" + 
              		"CR1bv/Lo0UHJtCMu0yXFhWd+PJCv8/Jv/VxCPbN9OHdnlwrUrytxTMCXTO7wv4mc\n" + 
              		"WNFLbhGPrQJmH53qtdnc0Lz0O5I1JuOvnf5a89PN4cNqmG8PZPp/e0u8KW1JsJ8j\n" + 
              		"ruF6j7JLXn7bQvYTvEFKoPT9NicmCFnwnwZEVz3TCFu/WfhMLVdotPMiiGqswztl\n" + 
              		"+mypzUGIMJ3muU2GXnU/7hZL2h7il2G7nWK1DHl/TENgfFTM6TuR1KNPEik54JYe\n" + 
              		"kafQuX7ER5dkw1jWaj7lzL7tKtuLeb59OPfZ3pkO7fuixv8=\n" + 
              		"-----END CERTIFICATE-----\n" + 
              		"",
            	        
            	        new BigInteger(130, new SecureRandom()).toString(32)
            	    );
              EnvKeyStore envKeyStore;// = EnvKeyStore.createWithRandomPassword("KAFKA_CLIENT_CERT_KEY", "KAFKA_CLIENT_CERT");

              envKeyStore =   new EnvKeyStore(
            	        "-----BEGIN RSA PRIVATE KEY-----\n" + 
            	        "MIIEpQIBAAKCAQEA7NzDhVEj12XodyuLqUiaxxApHfBDC8Tai8MDbEt4Eibaegs3\n" + 
            	        "6vhQ5c8y5mAjFjiBEaHW/Dyg16Hc0PYb+n//sgxh7HfXYIFbm+C4dY5n0LoHpAPR\n" + 
            	        "0WZCPC2hwReSfaTVOarpgJoUv+TX9ESvsuoRLQyrQehML17OZuI47uvCz7xIgu6h\n" + 
            	        "07mdU0Eo6XVeBWC03vHe4w8mjvIIPg3gp+QcH6dKDGF9CCkCcuSILFpm8Tctcu4U\n" + 
            	        "Iw6BiI4+rJ8w7InniTGdPMx2ckHiKK8o0BC34UBFQi1YoLR5Usmbo084OqVpBknp\n" + 
            	        "9f5t5HIjZ/yjjHtIM7JU/hWWsubnZURQgjE5BwIDAQABAoIBAGqgHqwfEKFgULuE\n" + 
            	        "sMQt2O8PiVLe22+q2C7eROY1OwIA14zlC/EMg25QTbuDP4g7O27yidLWgBlR1hOD\n" + 
            	        "Fqk3gaVnCZXnhN7+nHyZNrBEwNsnQ1VEGunGvL5WVGCV+e3xi8L/+0lXE0wm+kgN\n" + 
            	        "u4Iw+DZtY5Kxvcn9RbDu2W5EV1gnrjfeqg/f4I2vpwxn09MmejRluZqELYQ0iinv\n" + 
            	        "ZeNnBNwiqD4Xc7Bf7Q58lOS90gZ6yVdppX/ao4YHgxPLj6njmpv/LlmTlluntlLZ\n" + 
            	        "ZfkcS49kuxtye0xF1MjCYsBHHWuk6zD3tbUgZmGroOKH5bgT0TAB7I9TsQN4DDEs\n" + 
            	        "qwCnlZECgYEA/a63vDFTLyy3o/sZNWRuhi+fFckK0rY9gE1ErFQ5aDIRtLTRQuAU\n" + 
            	        "772z+qa+UE/Z7OiMFbmgEMfa56UwSyDegaMXaxE4ZhcxGH7371FErY/l3E5LA++6\n" + 
            	        "8pK+ZNHhGDc85xQzBN1FL4APkcwMytd+Qv1tpqfVFvHaTn+By6c8tT0CgYEA7wa1\n" + 
            	        "iQZUTuX+OT8apHspf2B7ON05n6qGEbLQ4HHAiGjgbl/GW8qkzKAtGsc0WjUHYeCX\n" + 
            	        "t1k0GWISzIJnSJnZsjqAFS5j8JGBwAJDGMb0regKv07ytQ5DmsovqdKB4JwcGs6t\n" + 
            	        "i7HpwSjbrsxm5eEklAaaPCqespxekRPG9dLCM5MCgYEAsdld4shJ41bcYFBX+gCx\n" + 
            	        "I84bH/DUb6loMJz7Oj3KCWyg0Sm7U8E5rGI43c8sua1hwR+/pjN/LoSOybwbwXAH\n" + 
            	        "zqcCcgOeoKQ9vUi2lcdJ+MxbgDo7iUT2sb2DUbd6sbl4LyEQK6bdLFIBmFuP1F2D\n" + 
            	        "nX+C8kXTtMRWIpZt7tMOUpkCgYEAhHhCacOBPAzJHS709A1yDS8Ke5RXmD6oeOyS\n" + 
            	        "SKGiY7dEEsevpjWjqehntvyJ7iiPg9Y2Hx4n+p1Y79ChryHc/aLgU1zXdH8f2qsm\n" + 
            	        "RngVKMB+HFKDvoY+P24ohkStSC8cgFrk4ZPjifK79Z9As8xYOlWCay/vrettmZfN\n" + 
            	        "X3XDvVMCgYEA4BFxcg+PiAMpl6XIKsDne4XscH4AnThieXmimVejOGnr2py9q75E\n" + 
            	        "nJPMjk3a4P+NzSi5HVl8HVR/71kk5TEJnxJQlbazNXz/bUyjitqoxtfCdQz2F6SK\n" + 
            	        "TS1sSs9gGWqtQ0PXP0jjorLWrgyXn7q2B1OG0nBHVyNdKDUdTZkWDrc=\n" + 
            	        "-----END RSA PRIVATE KEY-----\n" + 
            	        "",
            	        "-----BEGIN CERTIFICATE-----\n" + 
            	        "MIIDQzCCAiugAwIBAgIBADANBgkqhkiG9w0BAQsFADAyMTAwLgYDVQQDDCdjYS02\n" + 
            	        "NjhjNDhmZC04OTg1LTQwZTAtYjM1Ny0wYTdhNTNhYWRmMTEwHhcNMTkwMTIzMDky\n" + 
            	        "MDQ2WhcNMjkwMTIzMDkyMDQ2WjAZMRcwFQYDVQQDDA51MjA3aDQ4YW1ub3M4cDCC\n" + 
            	        "ASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAOzcw4VRI9dl6Hcri6lImscQ\n" + 
            	        "KR3wQwvE2ovDA2xLeBIm2noLN+r4UOXPMuZgIxY4gRGh1vw8oNeh3ND2G/p//7IM\n" + 
            	        "Yex312CBW5vguHWOZ9C6B6QD0dFmQjwtocEXkn2k1Tmq6YCaFL/k1/REr7LqES0M\n" + 
            	        "q0HoTC9ezmbiOO7rws+8SILuodO5nVNBKOl1XgVgtN7x3uMPJo7yCD4N4KfkHB+n\n" + 
            	        "SgxhfQgpAnLkiCxaZvE3LXLuFCMOgYiOPqyfMOyJ54kxnTzMdnJB4iivKNAQt+FA\n" + 
            	        "RUItWKC0eVLJm6NPODqlaQZJ6fX+beRyI2f8o4x7SDOyVP4VlrLm52VEUIIxOQcC\n" + 
            	        "AwEAAaN9MHswHQYDVR0OBBYEFFiE7IkMca10gXnWZXfSlJ/jVXnAMFoGA1UdIwRT\n" + 
            	        "MFGAFL8uckUlrhsu60BJ33LrnD+XWvuVoTakNDAyMTAwLgYDVQQDDCdjYS02Njhj\n" + 
            	        "NDhmZC04OTg1LTQwZTAtYjM1Ny0wYTdhNTNhYWRmMTGCAQAwDQYJKoZIhvcNAQEL\n" + 
            	        "BQADggEBADFWXoDa2o6XqRbR7HIUvTPVJCWrxS0ZXwHeA2MtuktYCEs5SKENN4BI\n" + 
            	        "T3XT7IUuMW9DtSXGk2HOTTR8FWrKePSA3RhgTDH0KvHbwGB0PbIvLyOtnvtPWyLn\n" + 
            	        "9nq9aK/Z9vp1U1+3/KS/w+JdwSaLdWWB1S4jUgpwjAwfl2ZCKdZhASEdboemSnO3\n" + 
            	        "MobfQmFmiXG+J3Jx3gszrqrb31qDWskH0Fi3B65Vgjvjb9DNEUDTujFSNxqzRpGL\n" + 
            	        "pZp42iQVH1DBC41mPZJ6vbz+uvAZbydHedBp1IJ17t2TujZjTVuuu/WGDuF4CvdL\n" + 
            	        "8ltWKc47SBpmU7p6cooCjkYpHpK4snw=\n" + 
            	        "-----END CERTIFICATE-----\n" + 
            	        "" + 
            	        "",
            	        new BigInteger(130, new SecureRandom()).toString(32)
            	    );
              	
              File trustStore = envTrustStore.storeTemp();
              File keyStore = envKeyStore.storeTemp();

              properties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, envTrustStore.type());
              properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStore.getAbsolutePath());
              properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, envTrustStore.password());
              properties.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, envKeyStore.type());
              properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStore.getAbsolutePath());
              properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, envKeyStore.password());
            } catch (Exception e) {
              throw new RuntimeException("There was a problem creating the Kafka key stores", e);
            }
            break;
          default:
            throw new IllegalArgumentException(format("unknown scheme; %s", uri.getScheme()));
        }
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, hostPorts.stream().collect(Collectors.joining(",")));
    return properties;
  }

  public String getTopic() {
    return topic;
  }

  public String getConsumerGroup() {
    return consumerGroup;
  }
}
