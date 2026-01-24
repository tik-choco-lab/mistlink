#include "converter.h"
#include <stdlib.h>
#include <string.h>

#define SAMPLE_RATE 48000
#define CHANNELS 2
#define OPUS_FRAME_SIZE 960  // 20ms at 48kHz
#define AAC_FRAME_SIZE 1024

TranscodeCtx* init_transcoder() {
    int err;
    TranscodeCtx *ctx = (TranscodeCtx *)calloc(1, sizeof(TranscodeCtx));
    
    ctx->opus_dec = opus_decoder_create(SAMPLE_RATE, CHANNELS, &err);
    
    aacEncOpen(&ctx->aac_enc, 0, CHANNELS);
    aacEncoder_SetParam(ctx->aac_enc, AACENC_AOT, 2); // AAC-LC
    aacEncoder_SetParam(ctx->aac_enc, AACENC_SAMPLERATE, SAMPLE_RATE);
    aacEncoder_SetParam(ctx->aac_enc, AACENC_CHANNELMODE, MODE_2);
    aacEncoder_SetParam(ctx->aac_enc, AACENC_BITRATE, 128000);
    aacEncoder_SetParam(ctx->aac_enc, AACENC_TRANSMUX, 0); // Raw
    aacEncEncode(ctx->aac_enc, NULL, NULL, NULL, NULL);

    ctx->pcm_buffer = (float *)calloc(AAC_FRAME_SIZE * CHANNELS * 2, sizeof(float));
    ctx->pcm_buffer_size = 0;

    return ctx;
}

int transcode_frame(TranscodeCtx *ctx, const unsigned char *in, int in_len, unsigned char *out, int out_max) {
    int decoded = opus_decode_float(ctx->opus_dec, in, in_len, 
                                   ctx->pcm_buffer + (ctx->pcm_buffer_size * CHANNELS), 
                                   OPUS_FRAME_SIZE, 0);
    ctx->pcm_buffer_size += decoded;

    if (ctx->pcm_buffer_size >= AAC_FRAME_SIZE) {
        AACENC_InArgs in_args = {0};
        AACENC_OutArgs out_args = {0};
        void *in_ptr = ctx->pcm_buffer;
        int in_id = IN_AUDIO_DATA;
        int in_size = AAC_FRAME_SIZE * CHANNELS * sizeof(float);
        int in_elem_size = sizeof(float);
        
        AACENC_BufDesc in_buf = { .numBufs = 1, .bufs = &in_ptr, .bufferIdentifiers = &in_id, .bufSizes = &in_size, .bufElSizes = &in_elem_size };
        
        void *out_ptr = out;
        int out_id = OUT_BITSTREAM_DATA;
        int out_size = out_max;
        int out_elem_size = 1;
        AACENC_BufDesc out_buf = { .numBufs = 1, .bufs = &out_ptr, .bufferIdentifiers = &out_id, .bufSizes = &out_size, .bufElSizes = &out_elem_size };

        in_args.numInSamples = AAC_FRAME_SIZE * CHANNELS;
        aacEncEncode(ctx->aac_enc, &in_buf, &out_buf, &in_args, &out_args);

        ctx->pcm_buffer_size -= AAC_FRAME_SIZE;
        memmove(ctx->pcm_buffer, ctx->pcm_buffer + (AAC_FRAME_SIZE * CHANNELS), ctx->pcm_buffer_size * CHANNELS * sizeof(float));

        return out_args.numOutBytes;
    }
    return 0;
}

void free_transcoder(TranscodeCtx *ctx) {
    opus_decoder_destroy(ctx->opus_dec);
    aacEncClose(&ctx->aac_enc);
    free(ctx->pcm_buffer);
    free(ctx);
}
