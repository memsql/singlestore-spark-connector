package com.memsql.superapp

import akka.pattern.ask
import akka.actor.ActorRef
import com.memsql.spark.context.{MemSQLSQLContext, MemSQLSparkContext}
import com.memsql.spark.etl.api.PipelineConfig
import com.memsql.superapp.api.{ApiActor, PipelineState, Pipeline}
import ApiActor._
import com.memsql.superapp.util.JarLoader
import org.apache.spark.streaming.StreamingContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}

object PipelineMonitor {
  def of(api: ActorRef,
         pipeline: Pipeline,
         sparkContext: MemSQLSparkContext,
         sqlContext: MemSQLSQLContext,
         streamingContext: StreamingContext): Option[PipelineMonitor] = {
    try {
      val clazz = JarLoader.loadClass(pipeline.jar, pipeline.main_class)
      //TODO does this pollute the classpath for the lifetime of the superapp?
      //TODO if an updated jar is appended to the classpath the superapp will always run the old version
      //distribute jar to all tasks run by this spark context
      sparkContext.addJar(pipeline.jar)
      val pipelineInstance = clazz.newInstance.asInstanceOf[ {def run(sc: StreamingContext, sqlContext: MemSQLSQLContext, config: PipelineConfig)}]
      Some(PipelineMonitor(api, pipeline.pipeline_id, pipeline.config, pipelineInstance, streamingContext, sqlContext))
    } catch {
      case e: Exception => {
        val errorMessage = s"Failed to load class for pipeline ${pipeline.pipeline_id}: $e"
        Console.println(errorMessage)
        e.printStackTrace
        val future = (api ? PipelineUpdate(pipeline.pipeline_id, PipelineState.ERROR, error = errorMessage)).mapTo[Try[Boolean]]
        future.map {
          case Success(resp) => //exit
          case Failure(error) => Console.println(s"Failed to update pipeline ${pipeline.pipeline_id} state to ERROR: $error")
        }
        None
      }
    }
  }
}

case class PipelineMonitor(api: ActorRef,
                           pipeline_id: String,
                           pipelineConfig: PipelineConfig,
                           pipelineInstance: {def run(sc: StreamingContext, sqlc: MemSQLSQLContext, config: PipelineConfig)},
                           streamingContext: StreamingContext,
                           sqlContext: MemSQLSQLContext) {
  private var exception: Exception = null

  private val thread = new Thread(new Runnable {
    override def run(): Unit = {
      try {
        Console.println(s"Starting pipeline $pipeline_id")
        val future = (api ? PipelineUpdate(pipeline_id, PipelineState.RUNNING)).mapTo[Try[Boolean]]
        future.map {
          case Success(resp) => pipelineInstance.run(streamingContext, sqlContext, pipelineConfig)
          case Failure(error) => Console.println(s"Failed to update pipeline $pipeline_id state to RUNNING: $error")
        }
      } catch {
        case e: InterruptedException => //exit
        case e: Exception => {
          exception = e
          Console.println(s"Unexpected exception: $e")
          val future = (api ? PipelineUpdate(pipeline_id, PipelineState.ERROR, error = e.toString)).mapTo[Try[Boolean]]
          future.map {
            case Success(resp) => //exit
            case Failure(error) => Console.println(s"Failed to update pipeline $pipeline_id state to ERROR: $error")
          }
        }
      }
    }
  })

  def start(): Unit = {
    thread.start
  }

  def isAlive(): Boolean = {
    thread.isAlive
  }

  def stop() = {
    thread.interrupt
    thread.join
  }
}
