package io.weaviate.spark

import technology.semi.weaviate.client.Config
import technology.semi.weaviate.client.WeaviateClient
import technology.semi.weaviate.client.base.Result
import technology.semi.weaviate.client.v1.misc.model.Meta


class Connection(test: String){
  def connect(): Unit = {
    val config = new Config("https", "firestore.semi.network")
    val client = new WeaviateClient(config)
    val meta = client.misc().metaGetter().run()
    if (meta.getError() == null) {
      printf("meta.hostname: %s\n", meta.getResult().getHostname())
      printf("meta.version: %s\n", meta.getResult().getVersion())
      printf("meta.modules: %s\n", meta.getResult().getModules())
    } else {
      printf("Error: %s\n", meta.getError().getMessages())
    }
  }
}
 
