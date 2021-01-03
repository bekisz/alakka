package org.montecarlo.utils

object System {

  def classPathSources(cl: ClassLoader): Array[java.net.URL] = cl match {
    case null => Array()
    case u: java.net.URLClassLoader => u.getURLs() ++ classPathSources(cl.getParent)
    case _ => classPathSources(cl.getParent)
  }

  //val  urls = classPathSources(getClass.getClassLoader)
  //println(urls.mkString("\n"))
}
