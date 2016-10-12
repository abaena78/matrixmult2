

package bigdata.matmult2

import java.io.File;
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import javassist.expr.Instanceof

import org.apache.spark.rdd._

/**
 * Multiplicacion de matrices no dispersas
 * No hace division por bandas
 */
object MatrixMult2 {

  /**
   * Retorna  TRUE si se va a correr el programa en modo depuracion.
   * En este modo se deja codigo adicional que puede afectar el rendimiento. Ejemplo: guardar archivos
   */
  def isDebug(): Boolean = { true }

  /**
   * lee un archivo correspondiente a una matriz.
   * Convierte las columnas en filas
   */
  def matColumnsToRows(file: RDD[String]): RDD[String] = {

    //      val file = sc.textFile(matFilePath)
    if (isDebug) file.foreach(line => println("linea: " + line))

    val mapColums = file.zipWithIndex.flatMap {
      case (row, rowIndex) => row.split(" +").zipWithIndex.map {
        case (value, columnIndex) => columnIndex -> value
      }
    }

    if (isDebug) mapColums.foreach(println)

    val columns = mapColums.groupByKey().sortByKey().values
    val columnsFormatted = columns.map(a => a.mkString(" "))

    columnsFormatted
  }

  /**
   * Divide cada fila o columna en grupos
   */
  def createGroups(index: String, ngroups: Integer, value: String): Array[(String, String)] = {
    val array = new Array[(String, String)](ngroups)
    val dataSplit = value.split(" ")

    val sizeGroup = dataSplit.size / ngroups
    var from = 0
    var until = from + sizeGroup

    //key = index,group
    for (i <- 0 until ngroups) {
      array(i) = (index + "," + (i + 1) -> dataSplit.slice(from, until).mkString(" "))
      from = until
      until = from + sizeGroup
    }
    array
  }

  /**
   * Retorna un array con las posibles conbinaciones de fila y columna.
   * Fila constante, columna variable
   */
  def getIndexColVar(indexRow: String, ncol: Integer, value: String): Array[(String, String)] = {
    val array = new Array[(String, String)](ncol)
    for (i <- 0 until ncol) {
      val indexArray = indexRow.split(",")
      array(i) = (indexArray(0) + "," + (i + 1) + "," + indexArray(1) -> value)
    }
    array
  }

  /**
   * Retorna un array con las posibles conbinaciones de fila y columna.
   * Columna constante, fila variable
   */
  def getIndexRowVar(indexCol: String, nrow: Integer, value: String): Array[(String, String)] = {
    val array = new Array[(String, String)](nrow)
    for (i <- 0 until nrow) {
      array(i) = ((i + 1) + "," + indexCol -> value)
    }
    array
  }

  /**
   * realiza la multiplicacion y suma de la matrix
   */
  def multiply(data: (String, String)): String = {
    if (isDebug) println(">>>>data_1:" + data._1)
    if (isDebug) println(">>>>data_2:" + data._2)
    val rowArray = data._1.split(" ")
    val colArray = data._2.split(" ")

    var acc = 0.0
    for (i <- 0 until rowArray.size) {
      acc += rowArray(i).toDouble * colArray(i).toDouble
    }
    acc.toString()
  }

  /**
   * programa principal de multiplicacion de matrices no dispersas
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("matrix-mult2").setMaster("local")
    val sc = new SparkContext(conf)

    /* *********************************************************************************************/
    //CONFIGIRACION !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    //Numero de grupos en los que se divide cada fila/columna
    val ngroups = 2;
    //rutas de archivos y directorios
    //      val mat1FilePath = "hdfs://localhost:8020/user/matrix/matriz_a.dat"
    //      val mat2FilePath = "hdfs://localhost:8020/user/matrix/matriz_b-128.dat"
    //      val directoryOutput = "hdfs://localhost:8020/user/matrix/out"
    val mat1FilePath = "data/matriz_a1.dat"
    val mat2FilePath = "data/matriz_b1.dat"
    val directoryOutput = "data/out"

    def deleteRecursively(file: File): Unit = {    
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
    }
    
    deleteRecursively(new File(directoryOutput))

    //    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:8020"), hadoopConf)
    //    try {
    //      hdfs.delete(new org.apache.hadoop.fs.Path(directoryOutput), true)
    //    } catch {
    //      case _: Throwable => {}
    //    }
    /* *********************************************************************************************/

    println("Inicia")

    //Lee archivo matriz-1
    val mat1 = sc.textFile(mat1FilePath)

    //Lee archivo matriz-2
    val mat2 = sc.textFile(mat2FilePath)

    //Convierte columnas en filas
    val mat2Transpose = matColumnsToRows(mat2)

    val nrows = mat1.count()
    val ncol = mat2Transpose.count()
    println("nrows=" + nrows + ", ncol=" + ncol)

    // Le asigna Indica a cada fila, y la mapea como key 
    // (1,1.0e0 4.0e0),(2,2.0e0 5.0e0),...
    val mat1KeyRow = mat1.zipWithIndex().map { case (k, v) => (v + 1, k) }
    val mat2KeyCol = mat2Transpose.zipWithIndex().map { case (k, v) => (v + 1, k) }

    if (isDebug) mat1KeyRow.foreach(println)
    if (isDebug) mat2KeyCol.foreach(println)

    //se divide cada fila/columna en los grupos. Se deja el k que corresponde al grupo en la parte final
    //1-> (1 2 3 4)    1,1->(1 2)    1,2->(3 4)    
    val mat1KeyRowGroup = mat1KeyRow.flatMap { case (k, v) => createGroups(k.toString(), ngroups, v) }
    if (isDebug) mat1KeyRowGroup.foreach(println)

    val mat2KeyColGroup = mat2KeyCol.flatMap { case (k, v) => createGroups(k.toString(), ngroups, v) }
    if (isDebug) mat2KeyColGroup.foreach(println)

    //Para cada fila, crea todas las combinaciones segun las multiplicaciones q debe realizar
    //Esto es para cada fila crea los key correspondientes a la posicios d ela matriz resultado
    //(1,1,g->1.0e0 4.0e0),(1,2,g->1.0e0 4.0e0),(1,3,g->1.0e0 4.0e0),(1,4,g->1.0e0 4.0e0),(2,1,g->2.0e0 5.0e0)    
    val mat1KeyRowCol = mat1KeyRowGroup.flatMap { case (k, v) => getIndexColVar(k.toString(), ncol.toInt, v) }
    if (isDebug) mat1KeyRowCol.foreach(println)

    val mat2KeyRowCol = mat2KeyColGroup.flatMap { case (k, v) => getIndexRowVar(k.toString(), nrows.toInt, v) }
    if (isDebug) mat2KeyRowCol.foreach(println)

    //hace un join de las dos matrices aprovechando el key generado en el paso anterior
    //Con esta queda una fila por cada posicion de la matriz resultado
    //El valor es una tupla (String, String). En una esta la fila de Mat-1 y en el otro la columna d ela mat-2
    //(2,1,g->(2.0e0 5.0e0,11.0e0 12.0e0))
    val matResultKeyRowCol = mat1KeyRowCol.join(mat2KeyRowCol)
    if (isDebug) matResultKeyRowCol.foreach(println)

    //Realiza la multiplicacion y suma de la fila por la columna correspondiente al grupo
    val resultGroup = matResultKeyRowCol.map { case (k, v) => (k -> multiply(v)) }.map { case (k, v) => (k.split(",").slice(0, 2).mkString(",") -> v.toDouble) }
    if (isDebug) resultGroup.foreach(println)

    val result = resultGroup.reduceByKey((x, y) => x + y)
    if (isDebug) result.foreach(println)

    result.saveAsTextFile(directoryOutput)
    println("Fin")

    sc.stop
  }
}