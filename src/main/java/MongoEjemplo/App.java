package MongoEjemplo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;

public class App {

	public static void main(String[] args) {
		String connectionMongoDb = "mongodb+srv://lapinosb:UrnM2ixLjANNP7UG@cluster0.q9pvitx.mongodb.net/?retryWrites=true&w=majority";
		try (MongoClient mongoClient = MongoClients.create(new ConnectionString(connectionMongoDb))) {
			System.out.println("Entra conexion");
			MongoDatabase mongoDatabase = mongoClient.getDatabase("mibasededatosluis");
			MongoCollection<Document> collection = mongoDatabase.getCollection("micoleccion");
			List<String> dniesAEliminar = Arrays.asList("987654321B");
			List<Document> nuevosDocumentos = new ArrayList<>();

			// Intentar crear el índice único para el campo DNI
			/*
			 * try { collection.createIndex(Indexes.ascending("dni"), new
			 * IndexOptions().unique(true)); } catch (MongoWriteException e) { // Si se
			 * produce una excepción de índice duplicado, imprime un mensaje y continúa la
			 * ejecución
			 * System.out.println("Error al crear el índice único para el campo DNI: " +
			 * e.getMessage()); }
			 */

			nuevosDocumentos.add(new Document("nombre", "Juan").append("edad", 30));
			nuevosDocumentos.add(new Document("nombre", "María").append("edad", 25));

			agregarMultiplesDocumentos(collection, nuevosDocumentos);

			borrarDocumentosPorDNI(collection, dniesAEliminar);

			// Crear un nuevo documento con un nombre, una edad, una ciudad y un DNI
			crearDocumento(collection, "Luis", 32, "Zafiro", "123456789A");

			// Mostrar todos los documentos en la colección
			mostrarDocumentos(collection);

			// Consultar un documento por DNI
			// consultarDocumentoPorDNI(collection, "123456789A");

			// Modificar un documento por DNI
			modificarDocumentoPorDNI(collection, "123456789A", "edad", 50);

			// Borrar un documento por DNI
			// borrarDocumentoPorDNI(collection, "987654321B");

			// Borrar varios documentos por DNI
			//borrarDocumentosPorDNI(collection, dniesAEliminar);

			// Mostrar nuevamente todos los documentos en la colección después de la eliminación
			mostrarDocumentos(collection);

			// Mostrar usuarios que no tienen DNI unicamente
			// mostrarUsuariosSinDNI(collection);

		} catch (Exception e) {
			// TODO: handle exception
			System.out.println("Error : " + e.getMessage());
		}
	}

	private static void agregarMultiplesDocumentos(MongoCollection<Document> collection, List<Document> documentos) {
		collection.insertMany(documentos);
		System.out.println("Se han agregado " + documentos.size() + " documentos correctamente.");
	}

	// Método para mostrar usuarios que no tienen DNI
	private static void mostrarUsuariosSinDNI(MongoCollection<Document> collection) {
		Document filtro = new Document("dni", new Document("$exists", false));
		MongoCursor<Document> cursor = collection.find(filtro).iterator();
		try {
			while (cursor.hasNext()) {
				System.out.println(cursor.next().toJson());
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			cursor.close();
		}
	}

	// Método para mostrar todos los documentos de la colección
	private static void mostrarDocumentos(MongoCollection<Document> collection) {
		MongoCursor<Document> cursor = collection.find().iterator();
		System.out.println("***********************************");
		try {
			while (cursor.hasNext()) {
				System.out.println(cursor.next().toJson());
			}
		} catch (Exception e) {
			e.printStackTrace();
			cursor.close();
		}
	}

	// Método para consultar un documento por DNI
	private static void consultarDocumentoPorDNI(MongoCollection<Document> collection, String dni) {
		Document filtro = new Document("dni", dni);
		Document documentoEncontrado = collection.find(filtro).first();
		if (documentoEncontrado != null) {
			System.out.println(documentoEncontrado.toJson());
		} else {
			System.out.println("No se encontró ningún documento con el DNI proporcionado.");
		}
	}

	private static void crearDocumento(MongoCollection<Document> collection, String nombre, int edad, String ciudad,
			String dni) {
		Document document = new Document("Nombre", nombre).append("edad", edad).append("ciudad", ciudad).append("dni",
				dni);
		collection.insertOne(document);
	}

	private static void modificarDocumentoPorDNI(MongoCollection<Document> collection, String dni,
			String campoModificar, Object nuevoValor) {
		Document filtro = new Document("dni", dni);
		Document actualizacion = new Document("$set", new Document(campoModificar, nuevoValor));
		collection.updateOne(filtro, actualizacion);
	}

	private static void borrarDocumentoPorDNI(MongoCollection<Document> collection, String dni) {
		Document filtro = new Document("dni", dni);
		long documentosBorrados = collection.deleteOne(filtro).getDeletedCount();
		if (documentosBorrados > 0) {
			System.out.println("Documento con DNI '" + dni + "' eliminado correctamente.");
		} else {
			System.out.println("No se encontró ningún documento con el DNI '" + dni
					+ "'. No se realizó ninguna operación de eliminación.");
		}
	}

	private static void borrarDocumentosPorDNI(MongoCollection<Document> collection, List<String> dnies) {
		Document filtro = new Document("dni", new Document("$in", dnies));
		DeleteResult result = collection.deleteMany(filtro);
		long documentosBorrados = result.getDeletedCount();
		System.out.println("Se eliminaron " + documentosBorrados + " documentos con los DNI especificados.");
	}

}
