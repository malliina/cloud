package gcp

import java.io.FileInputStream
import java.nio.file.Paths

import com.google.api.gax.core.FixedCredentialsProvider
import com.google.auth.Credentials
import com.google.auth.oauth2.GoogleCredentials

import scala.collection.JavaConverters.asJavaCollectionConverter

object GCP {
  val credentialsFile = Paths.get(sys.props("user.home")).resolve(".gcp").resolve("credentials.json")
  val credentialsProvider: FixedCredentialsProvider = new FixedCredentialsProvider {
    override def getCredentials: Credentials = credentials
  }

  def credentials = {
    val file = sys.env.get("GOOGLE_APPLICATION_CREDENTIALS").map(Paths.get(_)).getOrElse(credentialsFile)
    GoogleCredentials
      .fromStream(new FileInputStream(file.toFile))
      .createScoped(Seq("https://www.googleapis.com/auth/cloud-platform").asJavaCollection)
  }
}
