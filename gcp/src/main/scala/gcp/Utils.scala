package gcp

import java.text.Normalizer

import org.apache.commons.text.{CharacterPredicates, RandomStringGenerator}

object Utils {
  private val generator = new RandomStringGenerator.Builder()
    .withinRange('a', 'z')
    .filteredBy(CharacterPredicates.LETTERS)
    .build()

  def randomString(length: Int) = generator.generate(length).toLowerCase

  def normalize(input: String): String =
    Normalizer
      .normalize(input, Normalizer.Form.NFD)
      .replaceAll("[^\\p{ASCII}]", "")
      .toLowerCase
      .replaceAll("[^-a-zA-Z0-9]", "-")
      .trim
}
