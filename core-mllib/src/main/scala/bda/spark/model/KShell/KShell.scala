package bda.spark.model.KShell

import bda.common.Logging
import bda.common.util.Msg
import org.apache.spark.graphx.Graph

/** KShell class factory. */
object KShell extends Logging {


  /** KShell model user interface. */
  def run(g: Graph[Double, Double], dir: String = "indegree", weight: Boolean = false): KShellModel = {
    val msg = Msg("n(g.vertices)" -> g.vertices.count(),
      "n(g.edges)" -> g.edges.count,
      "dir" -> dir,
      "weight" -> weight)
    logInfo(msg.toString)

    val KShell = new KShellTrainer(dir, weight)
    KShell.run(g)
  }

}
