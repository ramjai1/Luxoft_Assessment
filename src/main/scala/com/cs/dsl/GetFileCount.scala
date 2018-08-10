package com.cs.dsl

import java.io.File

class GetFileCount {
  def getFilesCount(dir: String):Int = {
    val file = new File(dir)
    require(file.exists() && file.isDirectory)
    def inDirectoryFiles(inDir:List[File]):Int={
      if(inDir.filter(_.isDirectory).nonEmpty)
        inDir.filter(_.isFile).length + inDirectoryFiles(inDir.flatMap(_.listFiles.toList))
      else
        inDir.filter(_.isFile).length
    }
    file.listFiles.filter(_.isFile).toList.length + inDirectoryFiles(file.listFiles.filter(_.isDirectory).toList)
  }
}