package sttp.tapir.server.netty.internal

import io.netty.handler.codec.http.HttpContent
import io.netty.handler.codec.http.multipart.{HttpPostMultipartRequestDecoder, InterfaceHttpData}
import org.apache.http.HttpEntity
import org.apache.http.entity.ContentType
import org.apache.http.entity.mime.content._
import org.apache.http.entity.mime.{FormBodyPart, FormBodyPartBuilder, MultipartEntityBuilder}
import sttp.model.Part
import sttp.tapir.{RawBodyType, RawPart}

object MultipartUtil {
  implicit class DecoderOps(decoder: HttpPostMultipartRequestDecoder) {
    def decodeChunk(httpContent: HttpContent): Seq[InterfaceHttpData] = {
      decoder.offer(httpContent)
      Iterator.continually(maybeNext()).takeWhile(_.nonEmpty).flatten.toSeq
    }

    private def maybeNext(): Option[InterfaceHttpData] = if (decoder.hasNext) Some(decoder.next()) else None
  }

  def rawPartToMultipartFormEntity(m: RawBodyType.MultipartBody, parts: Seq[RawPart]): HttpEntity = {
    parts
      .foldLeft(MultipartEntityBuilder.create()) { (builder, part) =>
        rawPartToFormBodyPart(m, part).map(builder.addPart).getOrElse(builder)
      }
      .build()
  }

  private def rawPartToFormBodyPart[R](m: RawBodyType.MultipartBody, part: Part[R]): Option[FormBodyPart] = {
    m.partType(part.name).map { partType =>
      part.headers
        .foldLeft(
          FormBodyPartBuilder.create(
            part.name,
            rawValueToContentBody(partType.asInstanceOf[RawBodyType[R]], part)
          )
        )((b, h) => b.addField(h.name, h.value))
        .build()
    }
  }

  private def rawValueToContentBody[R](bodyType: RawBodyType[R], part: Part[R]): ContentBody = {
    val contentType: String = part.header("content-type").getOrElse("text/plain")
    val r = part.body

    bodyType match {
      case RawBodyType.StringBody(_) =>
        new StringBody(r.toString, ContentType.parse(contentType))
      case RawBodyType.ByteArrayBody =>
        new ByteArrayBody(r, ContentType.create(contentType), part.fileName.get)
      case RawBodyType.ByteBufferBody =>
        val array: Array[Byte] = new Array[Byte](r.remaining)
        r.get(array)
        new ByteArrayBody(array, ContentType.create(contentType), part.fileName.get)
      case RawBodyType.FileBody =>
        part.fileName match {
          case Some(filename) => new FileBody(r.file, ContentType.create(contentType), filename)
          case None           => new FileBody(r.file, ContentType.create(contentType))
        }
      case RawBodyType.InputStreamRangeBody =>
        new InputStreamBody(r.inputStream(), ContentType.create(contentType), part.fileName.get)
      case RawBodyType.InputStreamBody =>
        new InputStreamBody(r, ContentType.create(contentType), part.fileName.get)
      case _: RawBodyType.MultipartBody =>
        throw new UnsupportedOperationException("Nested multipart messages are not supported.")
    }
  }
}
