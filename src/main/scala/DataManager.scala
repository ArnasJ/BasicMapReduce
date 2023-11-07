import com.github.tototoshi.csv.CSVReader

import java.io.PrintWriter
import java.nio.file.{Files, Path}
import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable
import scala.collection.parallel.ParIterable

trait DataManager[K, V] {
  def serialize(key: K, values: Seq[V]): Seq[String]

  def readAndMapFilesPar(
    dir: Path,
    mapperFunc: Seq[Map[String, String]] => Seq[(K, V)]
  ): ParIterable[(K, V)]

  def writeToFile(outPath: Path, fileIndex: Int, data: Seq[String]): Unit

  def getFilesPaths(rootPath: Path): Seq[String]
}

/**
 * A class that provides utility methods for reading, mapping, serializing, and writing CSV data.
 *
 * @param serializeFunc The serialization function to convert aggregated data to strings.
 * @tparam K The key type for mapping.
 * @tparam V The value type for mapping.
 */
class CSVDataManager[K, V](serializeFunc: (K, Seq[V]) => Seq[String]) extends DataManager[K, V] {
  /**
   * Serializes aggregated data into strings.
   *
   * @param key    The key associated with the data.
   * @param values The values to be serialized.
   * @return A sequence of serialized data as strings.
   */
  def serialize(key: K, values: Seq[V]): Seq[String] = serializeFunc(key, values)

  /**
   * Reads and maps CSV files in parallel from a specified directory.
   *
   * @param dir        The directory containing CSV files.
   * @param mapperFunc The mapping function to transform CSV data into key-value pairs.
   * @return A parallel iterable of key-value pairs obtained from CSV files.
   */
  def readAndMapFilesPar(
    dir: Path,
    mapperFunc: Seq[Map[String, String]] => Seq[(K, V)]
  ): ParIterable[(K, V)] =
    getFilesPaths(dir)
      .par
      .flatMap(filePath => {
        val reader = CSVReader.open(filePath)
        val parsedFile = reader.allWithHeaders()
        reader.close()

        mapperFunc(parsedFile)
      })

  /**
   * Writes data to a file in the specified directory.
   *
   * @param outPath   The directory where the file should be written.
   * @param fileIndex The number of the file to write the data to.
   * @param data      The data to write to the file.
   */
  def writeToFile(outPath: Path, fileIndex: Int, data: Seq[String]): Unit = {
    def fileName = f"result-part-$fileIndex%03d.csv"
    val filePath = outPath.resolve(fileName)
    Files.createDirectories(outPath)
    val printWriter = new PrintWriter(filePath.toFile)
    data.filter(_.nonEmpty).foreach(line => printWriter.println(line))
    printWriter.close()
  }

  /**
   * Retrieves the paths of files in the specified directory.
   *
   * @param rootPath The root directory containing the files.
   * @return A sequence of file paths within the specified directory.
   */
  def getFilesPaths(rootPath: Path): Seq[String] =
    rootPath.toFile
      .listFiles
      .map(_.getPath)
      .toSeq
}