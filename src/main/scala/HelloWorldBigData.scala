import scala.collection.mutable._

object HelloWorldBigData {

 /*Premier programme en Scala*/

  val ma_var_imm : String = "Raphaël" // variable immutable, on ne peyt pas changer la valeur
  private val une_var_imm : String = "Une formation en Big Data" // Variable à portée privée

  class Person (var nom : String, var prenom : String, var age : Int)

  def main(args: Array[String]): Unit = {
    println("Hello World : Voici mon premier programme en Scala")

    var test_addition : Int = 15 // variable où l'on peut changer la valeur
    test_addition = test_addition + 10

    println(test_addition)

    println("Votre texte contient : " + Comptage_caracteres("qu'avez vous fait ce matin ?  ") + " caractères !")
    println("Le chox des syntaxes ! Cette méthode qui est égal à : " + Comptage_caracteres("je suis dieu") +
      ", équivaut à celle la : " + Comptage_caracteres2("je suis dieu") +
      ", qui équivaut à celle la : " + Comptage_caracteres3("je suis dieu"))

    // Utilisation des if
    // getResultat(10)

    // Utilisation des boucles While
    // testWhile(10)

    // Utilisation des for
    // testFor()

    collectionScala()
    collectionTuple()

  }
  // Syntaxe 1
  def Comptage_caracteres(texte : String) : Int = {
    texte.trim.length()
  }
  // Syntaxe 2
  def Comptage_caracteres2(texte: String): Int = {
    return texte.trim.length()
  }
  // Syntaxe 3
  def Comptage_caracteres3(texte: String): Int = texte.trim.length()

  def getResultat (parametre : Any) : Unit = {
    if (parametre == 10) {
      println("Votre valeur est un entier")
    } else {
      println("Votre valeur n'est pas un entier")
    }
  }

  def testWhile(valeur_cond : Int) : Unit = {
    var i : Int = 0
    while (i < valeur_cond) {
      println("Itération While N° " + i)
      i = i + 1
    }
  }

  def testFor () : Unit = {
    var i : Int = 0
    for (i <- 5 to 15) {
      println("Itération For N° " + i)
    }
  }

  def collectionScala () : Unit = {

    val maliste : List[Int] = List(1,2,3,4,5,6,7,8,9)
    val liste_s : List[String] = List("Jean","Philippe","Marc","Arnaud","Pierre","Julien","Arthur","Louis","Barn")
    val plage_v : List[Int] = List.range(1, 15, 2)

    println(maliste(0))

    for(i <- liste_s) {
      println(i)
    }

    //manipulation des collections à l'aide des fonctions anonymes
    val resultats : List[String] = liste_s.filter(e => e.endsWith("n"))

    println("Voici les noms qui finissent par un 'n' : ")

    for (r <- resultats) {
      print(r + " ")
    }
    println("")
    val res : Int = liste_s.count(i => i.endsWith("n"))

    println("Nombre d'éléments respectant la condition : " + res)

    val maliste2 : List[Int] = maliste.map(e => e * 2) // On peut remplacer 'e => e' par '_'
    // val maliste2 : List[Int] = maliste.map(_ * 2) // Comme ceci !
    for(r <- maliste2) {
      print(r + " ")
    }

    val maliste3 : List[Int] = maliste.map((e:Int)=> e * 2)

    val nouvelle_liste : List[Int] = plage_v.filter(p => p > 5)

    val new_list : List[String] = liste_s.map(s => s.capitalize)

    println("")
    println("Nouvelle liste avec les premières lettre en majuscule : ")
    // Permet d'afficher ce qu'il y a dans une liste à la suite
    new_list.foreach(e => print(e + " "))
    // plage_v.foreach(println(_)) // un autre exemple
  }

  def collectionTuple() : Unit = {

    val tuple_test = (45, "RDC", "False")
    print(tuple_test._3)

    val nouv_person : Person = new Person ("De Castro", "Raphaël", 21)

    val tuple_test2 = ("test", nouv_person, 67)
    tuple_test2.toString().toList

    // table de hachage
    val states = Map(
      "AK" -> "Alaska",
      "IL" -> "Illinois",
      "KY" -> "Kentucky"
    )

    val personne = Map(
      "nom" -> "De Castro",
      "prénom" -> "Raphaël",
      "age" -> 21
    )

    // les tableaux
    println("")
    val montableau : Array[String] = Array("RDC", "CDR", "DCR")
    montableau.foreach(e => print(e + " "))
  }
}
