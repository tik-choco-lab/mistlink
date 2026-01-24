#ifndef CONVERTER_H
#define CONVERTER_H

#include <opus/opus.h>
#include <fdk-aac/aacenc_lib.h>

typedef struct {
    OpusDecoder *opus_dec;
    HANDLE_AACENCODER aac_enc;
    float *pcm_buffer;
    int pcm_buffer_size;
} TranscodeCtx;

TranscodeCtx* init_transcoder();
void free_transcoder(TranscodeCtx *ctx);
int transcode_frame(TranscodeCtx *ctx, const unsigned char *in, int in_len, unsigned char *out, int out_max);

#endif
