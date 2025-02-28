package com.singularity_code.live_location.data

import android.content.Context
import android.util.Log
import arrow.core.Either
import com.singularity_code.live_location.util.ErrorMessage
import com.singularity_code.live_location.util.defaultOkhttp
import com.singularity_code.live_location.util.isConnected
import com.singularity_code.live_location.util.websocket
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import okhttp3.MediaType.Companion.toMediaTypeOrNull
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.WebSocket


interface Repository {
    val url: String
    val headers: HashMap<String, String>
    val context: Context

    fun openConnection()

    fun closeConnection()

    suspend fun sendData(
        data: String
    ): Either<ErrorMessage, String>
}

class WebSocketRepository(
    override val url: String,
    override val headers: HashMap<String, String>,
    override val context: Context
) : Repository {

    private lateinit var webSocket: WebSocket

    private var socketPendingJob: Job? = null
//    private var isConnected: Boolean = false

    override fun openConnection() {
        socketPendingJob?.cancel()

        CoroutineScope(Dispatchers.IO).launch {
            websocket(
                context = context,
                apiURL = url,
                headers = headers
            ).collect{
                webSocket = it
            }
        }
    }

    override fun closeConnection() {
        webSocket.close(1000, "normal closure")
    }

    override suspend fun sendData(data: String): Either<ErrorMessage, String> {
        return kotlin.runCatching {
            if (!webSocket.isConnected) openConnection()

            val result = webSocket.send(data)
            Either.Right(result.toString())
        }.getOrElse {
            Either.Left(it.message ?: it.cause?.message ?: "unknown error")
        }
    }

}

class RestfulRepository(
    override val url: String,
    override val headers: HashMap<String, String>,
    override val context: Context
) : Repository {

    private val okHttpClient by lazy {
        defaultOkhttp(context)
    }

    override fun openConnection() {
        // nothing to do
    }

    override suspend fun sendData(
        data: String
    ): Either<ErrorMessage, String> {
        val requestBody: RequestBody = RequestBody.create("text/plain".toMediaTypeOrNull(), data)

        val request: Request = Request.Builder()
            .apply {
                url(url)
                post(requestBody)
                headers.forEach {
                    addHeader(it.key, it.value)
                }
            }
            .build()

        return runCatching {
            val response = okHttpClient.newCall(request).execute()
            if (response.isSuccessful) {
                Either.Right(response.body?.string() ?: "nothing to show")
            } else {
                Either.Left(response.message)
            }
        }.getOrElse {
            Either.Left(it.message ?: it.cause?.message ?: "unknown error")
        }
    }

    override fun closeConnection() {
        // nothing to do
    }

}