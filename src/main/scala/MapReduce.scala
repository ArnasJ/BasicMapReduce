import java.nio.file.Path
import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable
import scala.util.{Failure, Success, Try}

/**
 * * A simple MapReduce framework for processing and aggregating data.
 *
 *                             ┌───────────┐
 *                             │   Input   │
 *                             └─────┬─────┘
 *                    ┌──────────────┼──────────────┐
 *                    ▼              ▼              ▼
 *              ┌───────────┐  ┌───────────┐  ┌───────────┐
 *              │  Mapper 1 │  │  Mapper 2 │  │  Mapper n │
 *              └───────────┘  └───────────┘  └───────────┘
 *                    └──────────────┼──────────────┘
 *                                   ▼
 *                        ┌─────────────────────┐
 *                        │ Merging and Sorting │
 *                        └──────────┬──────────┘
 *                    ┌──────────────┼──────────────┐
 *                    ▼              ▼              ▼
 *              ┌───────────┐  ┌───────────┐  ┌───────────┐
 *              │ Reducer 1 │  │ Reducer 2 │  │ Reducer n │
 *              └───────────┘  └───────────┘  └───────────┘
 *                    │              │              │
 *                    ▼              ▼              ▼
 *              ┌───────────┐  ┌───────────┐  ┌───────────┐
 *              │  Writer 1 │  │  Writer 2 │  │  Writer n │
 *              └───────────┘  └───────────┘  └───────────┘
 *                    │              │              │
 *                    ▼              ▼              ▼
 *              ┌───────────┐  ┌───────────┐  ┌───────────┐
 *              │  Output 1 │  │  Output 2 │  │  Output n │
 *              └───────────┘  └───────────┘  └───────────┘
 *
 *
 * This framework supports mapping data from multiple files, reducing mapped data based on a key,
 * and serializing the results to output files.
 *
 * @param chunkSize   The size of each reduce job.
 * @param ordering    The ordering used to sort data by keys.
 * @param dataManager The manager for reading, writing, and serializing files.
 */
class MapReduce[MappedK, MappedV](
  chunkSize: Int,
  ordering: Ordering[MappedK],
  dataManager: DataManager[MappedK, MappedV]
) {
  /**
   * Apply MapReduce processing to input data.
   *
   * @param mappers         A map of input data sources and their corresponding mapping functions.
   * @param reduceFunc      The reduce function that aggregates mapped data.
   * @param outputDirectory The directory where the output data files will be written.
   */
  def apply(
    mappers: Map[Path, Seq[Map[String, String]] => Seq[(MappedK, MappedV)]],
    reduceFunc: (MappedK, Seq[MappedV]) => Seq[MappedV],
    outputDirectory: Path,
  ): Unit = {
    Try {
      val mappedResults: Seq[(MappedK, MappedV)] = mappers
        .toSeq
        .flatMap {
          case (path, mapperFunc) => dataManager.readAndMapFilesPar(path, mapperFunc)
        }

      val mergedAndSortedResults = mappedResults
        .groupBy(_._1)
        .map(group => (group._1, group._2.map(_._2)))
        .toSeq
        .sortBy(tpl => tpl._1)(ordering)

      mergedAndSortedResults
        .grouped(chunkSize)
        .zipWithIndex
        .toVector
        .par
        .foreach {
          case (chunk, idx) =>
            val reducedChunk = chunk.map {
              case (k, value) => (k, reduceFunc(k, value))
            }

            val output = reducedChunk.flatMap {
              case (k, value) => dataManager.serialize(k, value)
            }

            if (output.nonEmpty) {
              dataManager.writeToFile(outputDirectory, idx + 1, output)
            }
        }
    } match {
      case Success(_) => println("MapReduce completed successfully.")
      case Failure(exception) => println(s"MapReduce failed: ${exception.getMessage}")
    }
  }
}