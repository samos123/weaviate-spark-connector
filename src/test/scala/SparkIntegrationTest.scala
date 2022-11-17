package io.weaviate.spark

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import technology.semi.weaviate.client.v1.schema.model.{Property, WeaviateClass}

import scala.jdk.CollectionConverters.{mapAsJavaMapConverter, seqAsJavaListConverter}
import scala.sys.process._

case class Article(title: String, content: String, wordCount: Int)

case class ArticleWithVector(title: String, content: String, wordCount: Int, vector: Array[Float])

case class ArticleDifferentOrder(content: String, wordCount: Int, title: String)

case class ArticleWithID(idCol: String, title: String, content: String, wordCount: Int)

case class ArticleWithExtraCols(title: String, author: String, content: String, wordCount: Int, ifpaRank: Int)

class SparkIntegrationTest
  extends AnyFunSuite
    with SparkSessionTestWrapper
    with BeforeAndAfter {
  val options: CaseInsensitiveStringMap =
    new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost:8080").asJava)
  val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)
  val client = weaviateOptions.getClient()
  val logger = ProcessLogger(
    (o: String) => println("out " + o),
    (e: String) => println("err " + e))

  var retries = 3

  def createClass(): Unit = {
    val clazz = WeaviateClass.builder.className("Article")
      .description("Article test class")
      .properties(Seq(
        Property.builder()
          .dataType(List[String]("string").asJava)
          .name("title")
          .build(),
        Property.builder()
          .dataType(List[String]("string").asJava)
          .name("content")
          .build(),
        Property.builder()
          .dataType(List[String]("int").asJava)
          .name("wordCount")
          .build()
      ).asJava).build

    val results = client.schema().classCreator().withClass(clazz).run
    if (results.hasErrors) {
      println("insert error" + results.getError.getMessages)
      if (retries > 1) {
        retries -= 1
        println("Retrying to create class in 5 seconds..")
        Thread.sleep(5000)
        createClass()
      }
    }
    println("Results: " + results.toString)
    retries = 3
  }

  def deleteClass(): Unit = {
    val result = client.schema().classDeleter()
      .withClassName("Article")
      .run()
    if (result.hasErrors) println("Error deleting class Article " + result.getError.getMessages)
  }

  before {
    val docker_run =
      """docker run -d --name=weaviate-test-container-will-be-deleted
    -p 8080:8080
    -e QUERY_DEFAULTS_LIMIT=25
    -e AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true
    -e DEFAULT_VECTORIZER_MODULE=none
    -e CLUSTER_HOSTNAME=node1
    -e PERSISTENCE_DATA_PATH=./data
    semitechnologies/weaviate:1.16.1"""
    val exit_code = docker_run ! logger
    assert(exit_code == 0)
  }

  after {
    "docker stop weaviate-test-container-will-be-deleted" ! logger
    "docker rm weaviate-test-container-will-be-deleted" ! logger
  }


  test("Article with strings and int") {
    createClass()
    import spark.implicits._
    val articles = Seq(Article("Sam", "Sam and Sam", 3)).toDF

    articles.write
      .format("io.weaviate.spark.Weaviate")
      .option("scheme", "http")
      .option("host", "localhost:8080")
      .option("className", "Article")
      .mode("append")
      .save()

    val results = client.data().objectsGetter()
      .withClassName("Article")
      .run()

    if (results.hasErrors) {
      println("Error getting Articles" + results.getError.getMessages)
    }

    val props = results.getResult.get(0).getProperties
    assert(props.get("title") == "Sam")
    assert(props.get("content") == "Sam and Sam")
    assert(props.get("wordCount") == 3)
    assert(results.getResult.size == 1)
    deleteClass()
  }

  test("Test empty strings and large batch") {
    createClass()
    import spark.implicits._
    val articles = (1 to 22).map(_ => Article("", "", 0)).toDF

    articles.write
      .format("io.weaviate.spark.Weaviate")
      .option("scheme", "http")
      .option("host", "localhost:8080")
      .option("className", "Article")
      .option("batchSize", 10)
      .mode("append")
      .save()

    val results = client.data().objectsGetter()
      .withClassName("Article")
      .run()

    if (results.hasErrors) {
      println("Error getting Articles" + results.getError.getMessages)
    }
    assert(results.getResult.size == 22)
    val props = (0 to 22 - 1).map(i => results.getResult.get(i).getProperties)
    results.getResult.forEach(obj => {
      assert(obj.getProperties.get("wordCount") == 0)
      assert(obj.getProperties.get("content") == "")
      assert(obj.getProperties.get("title") == "")
    })
    deleteClass()
  }

  test("Article different order") {
    createClass()
    import spark.implicits._
    val articles = Seq(ArticleDifferentOrder("Sam and Sam", 3, "Sam")).toDF

    articles.write
      .format("io.weaviate.spark.Weaviate")
      .option("scheme", "http")
      .option("host", "localhost:8080")
      .option("className", "Article")
      .mode("append")
      .save()

    val results = client.data().objectsGetter()
      .withClassName("Article")
      .run()

    if (results.hasErrors) {
      println("Error getting Articles" + results.getError.getMessages)
    }

    val props = results.getResult.get(0).getProperties
    assert(props.get("title") == "Sam")
    assert(props.get("content") == "Sam and Sam")
    assert(props.get("wordCount") == 3)
    assert(results.getResult.size == 1)
    deleteClass()
  }

  test("Article with Spark provided vectors") {
    createClass()
    import spark.implicits._
    val articles = Seq(ArticleWithVector("Sam", "Sam and Sam", 3, Array(0.01f, 0.02f))).toDF

    articles.write
      .format("io.weaviate.spark.Weaviate")
      .option("scheme", "http")
      .option("host", "localhost:8080")
      .option("className", "Article")
      .option("vector", "vector")
      .mode("append")
      .save()

    val results = client.data().objectsGetter()
      .withClassName("Article")
      .withVector()
      .run()

    if (results.hasErrors) {
      println("Error getting Articles" + results.getError.getMessages)
    }

    assert(results.getResult.get(0).getVector sameElements Array(0.01f, 0.02f))
    val props = results.getResult.get(0).getProperties
    assert(props.get("title") == "Sam")
    assert(props.get("content") == "Sam and Sam")
    assert(props.get("wordCount") == 3)
    assert(results.getResult.size == 1)
    deleteClass()
  }

  test("Article with custom IDs") {
    createClass()
    import spark.implicits._
    val id = java.util.UUID.randomUUID.toString
    val articles = Seq(ArticleWithID(id, "Sam", "Sam and Sam", 3)).toDF

    articles.write
      .format("io.weaviate.spark.Weaviate")
      .option("scheme", "http")
      .option("host", "localhost:8080")
      .option("className", "Article")
      .option("id", "idCol")
      .mode("append")
      .save()

    val results = client.data().objectsGetter()
      .withClassName("Article")
      .run()

    if (results.hasErrors) {
      println("Error getting Articles" + results.getError.getMessages)
    }

    assert(results.getResult.get(0).getId == id)
    val props = results.getResult.get(0).getProperties
    assert(props.get("title") == "Sam")
    assert(props.get("content") == "Sam and Sam")
    assert(props.get("wordCount") == 3)
    assert(results.getResult.size == 1)
  }

  test("Article with extra columns") {
    createClass()
    import spark.implicits._
    val articles = Seq(ArticleWithExtraCols("Sam", "Big Dog", "Sam and Sam", 3, 1900)).toDF

    assertThrows[AnalysisException] {
      articles.write
        .format("io.weaviate.spark.Weaviate")
        .option("scheme", "http")
        .option("host", "localhost:8080")
        .option("className", "Article")
        .mode("append")
        .save()
    }
  }
}