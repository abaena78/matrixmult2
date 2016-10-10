

package bigdata.matmult2

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
   * Retorna si se va a correr el programa en modo depuracion.
   * En este modo se dejan codigo adicional que puede facetar el rendimiento. Ejemplo: guardar archivos
   */
    def isDebug(): Boolean = { true }
    
    /**
     * lee un archivo correspondiente a una matriz.
     * Convierte las columnas en filas
     */
    def matColumnsToRows(sc: SparkContext, matFilePath: String): RDD[String] = {
      
      val file = sc.textFile(matFilePath)
      if(isDebug) file.collect().foreach(line => println("linea: "+line))
        
      val mapColums = file.zipWithIndex.flatMap{ 
        case (row, rowIndex) => row.split(" +").zipWithIndex.map { 
          case (value, columnIndex) => columnIndex -> value
        } 
      }
  
      if(isDebug) mapColums.foreach(println)
      
      val columns = mapColums.groupByKey().sortByKey().values
      val columnsFormatted = columns.map( a => a.mkString(" "))
      
      //if(isDebug)columnsFormatted.saveAsTextFile("data/tmp")     
       
      columnsFormatted      
    }
    
    
    def createGroups(index: String, ngroups: Integer, value: String): Array[(String, String)] = {
      
    }
    
    /**
     * Retorna un array con las posibles conbinaciones de fila y columna. 
     * Fila constante, columna variable
     */
    def getIndexColVar(indexRow: String, ncol: Integer, value: String): Array[(String, String)] = {
      val array = new Array[(String, String)](ncol)
      for(i <- 0 until ncol) {
        array(i) = (indexRow+","+(i+1) -> value)
      }
      array
    }
 
    /**
     * Retorna un array con las posibles conbinaciones de fila y columna. 
     * Columna constante, fila variable
     */    
    def getIndexRowVar(indexCol: String, nrow: Integer, value: String): Array[(String, String)] = {
      val array = new Array[(String, String)](nrow)
      for(i <- 0 until nrow) {
        array(i) = ((i+1)+","+indexCol -> value)
      }
      array
    }  
    
    /**
     * realiza la multiplicacion y suma de la matrix
     */
    def multiply(data: (String, String)): String = {
      if(isDebug)println(">>>>data_1:"+data._1)
      if(isDebug)println(">>>>data_2:"+data._2)
      val rowArray = data._1.split(" ")
      val colArray = data._2.split(" ")
     
      var acc = 0.0
      for(i <- 0 until rowArray.size) {
        acc += rowArray(i).toDouble*colArray(i).toDouble
      }
      acc.toString()     
    }      
  
    /**
     * programa principal de multiplicacion de matrices no dispersas
     */
    def main(args: Array[String]): Unit = {
    println("Inicia")
    
    //Inicia Spark context
    val conf = new SparkConf().setAppName("matrix-mult2").setMaster("local")
    val sc = new SparkContext(conf) 
    
    //Numero de grupos en los que se divide cada fila/columna
    val ngroups = 2;
    
    //Lee archivo matriz-1
    val mat1 = sc.textFile("data/matriz_a1.dat")
    //var mat1 = sc.textFile("hdfs://localhost:8020/user/matrix/matriz_a.dat")  
    
    //Lee archivo matriz-1. Convierte columnas en filas
    val mat2 = matColumnsToRows(sc, "data/matriz_b1.dat")
    //val mat2 = matColumnsToRows(sc, "hdfs://localhost:8020/user/matrix/matriz_b-128.dat")
    
    val nrows = mat1.count()
    val ncol = mat2.count()
    println("nrows="+nrows +", ncol="+ncol)
    
    // Le asigna Indica a cada fila, y la mapea como key 
    // (1,1.0e0 4.0e0),(2,2.0e0 5.0e0),...
    val mat1KeyRow = mat1.zipWithIndex().map{ case (k, v) => (v+1, k)} 
    val mat2KeyCol = mat2.zipWithIndex().map{ case (k, v) => (v+1, k)} 

    if(isDebug) mat1KeyRow.foreach(println)
    if(isDebug) mat2KeyCol.foreach(println)
    
    
    //se divide cada fila/columna en los grupos
    //1,1 2 3 4    1,1,1 2    1,2 3 4
    
    val mat1KeyRowGroup = mat1KeyRow.flatMap{ case (k, v) =>  createGroups(k.toString(), ngroups, v) } 
    
    
    //Para cada fila, crea todas las combinaciones segun las multiplicaciones q debe realizar
    //Esto es para cada fila crea los key correspondientes a la posicios d ela matriz resultado
    //(1,1,1.0e0 4.0e0),(1,2,1.0e0 4.0e0),(1,3,1.0e0 4.0e0),(1,4,1.0e0 4.0e0),(2,1,2.0e0 5.0e0)    
    val mat1KeyRowCol = mat1KeyRow.flatMap{ case (k, v) =>  getIndexColVar(k.toString(), ncol.toInt, v) }  
    if(isDebug) mat1KeyRowCol.foreach(println)
    
    val mat2KeyRowCol = mat2KeyCol.flatMap{ case (k, v) =>  getIndexRowVar(k.toString(), nrows.toInt, v) }  
    if(isDebug) mat2KeyRowCol.foreach(println)
    
    //hace un join de las dos matrices aprovechando el key generado en el paso anterior
    //Con esta queda una fila por cada posicion de la matriz resultado
    //El valor es una tupla (String, String). En una esta la fila de Mat-1 y en el otro la columna d ela mat-2
    //(2,1,(2.0e0 5.0e0,11.0e0 12.0e0))
    val matResultKeyRowCol = mat1KeyRowCol.join(mat2KeyRowCol)  
    println("matJoin:")
    if(isDebug) matResultKeyRowCol.foreach(println)
    
    //Realiza la multiplicacion y suma de la fila por la columna
    val result =  matResultKeyRowCol.map{ case (k, v) => (k -> multiply(v)) }
    if(isDebug) result.foreach(println)
    
    result.saveAsTextFile("data/out") 
    
    sc.stop     
    println("Fin")
  }
}