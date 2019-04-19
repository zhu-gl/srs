/*
The MIT License (MIT)

Copyright (c) 2013-2015 SRS(ossrs)

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

#include <srs_app_ingest.hpp>

#ifdef SRS_AUTO_INGEST

#include <stdlib.h>
using namespace std;

#include <srs_kernel_error.hpp>
#include <srs_app_config.hpp>
#include <srs_kernel_log.hpp>
#include <srs_app_ffmpeg.hpp>
#include <srs_app_pithy_print.hpp>
#include <srs_kernel_utility.hpp>
#include <srs_app_utility.hpp>

// when error, ingester sleep for a while and retry.
// ingest never sleep a long time, for we must start the stream ASAP.
#define SRS_AUTO_INGESTER_SLEEP_US (int64_t)(3*1000*1000LL)

SrsIngesterFFMPEG::SrsIngesterFFMPEG()
{
#if 1//def __SRS_DYNAMIC__
    channel = -1;
    ff_active = false;
#endif
    ffmpeg = NULL;
}

SrsIngesterFFMPEG::~SrsIngesterFFMPEG()
{
    srs_freep(ffmpeg);
}

#if 1//def __SRS_DYNAMIC__
std::string SrsIngesterFFMPEG::out_url()
{
    if (ffmpeg) {
        return ffmpeg->output();
    }

    return "";
}

unsigned int SrsIngesterFFMPEG::channel_out()
{
    return channel;
}

void SrsIngesterFFMPEG::active(bool b_active)
{
    srs_trace("Ingester: %d active: %d", channel, b_active);
    ff_active = b_active;
}

bool SrsIngesterFFMPEG::active()
{
    return ff_active;
}

int SrsIngesterFFMPEG::initialize(SrsFFMPEG* ff, std::string v, std::string i, SrsRequestParam& pm, unsigned int chl)
{
    int ret = ERROR_SUCCESS;

    srs_trace("src id: %s app: %s channel: %d", v.c_str(), i.c_str(), chl);

    req_param = pm;
    channel = chl;
    ffmpeg = ff;
    vhost = v;
    id = i;
    starttime = srs_get_system_time_ms();

    return ret;
}
#else
int SrsIngesterFFMPEG::initialize(SrsFFMPEG* ff, string v, string i)
{
    int ret = ERROR_SUCCESS;
    
    ffmpeg = ff;
    vhost = v;
    id = i;
    starttime = srs_get_system_time_ms();
    
    return ret;
}
#endif

string SrsIngesterFFMPEG::uri()
{
    return vhost + "/" + id;
}

int SrsIngesterFFMPEG::alive()
{
    return (int)(srs_get_system_time_ms() - starttime);
}

#if 1//def __SRS_DYNAMIC__
bool SrsIngesterFFMPEG::equals(std::string v, std::string i, SrsRequestParam& pm)
{
    return vhost == v && id == i
        && (pm.ip == req_param.ip) && (pm.channel == req_param.channel)
        && (pm.starttime == req_param.starttime) && (pm.endtime == req_param.endtime);
}
#endif

bool SrsIngesterFFMPEG::equals(string v)
{
    return vhost == v;
}

bool SrsIngesterFFMPEG::equals(string v, string i)
{
    return vhost == v && id == i;
}

int SrsIngesterFFMPEG::start()
{
    return ffmpeg->start();
}

void SrsIngesterFFMPEG::stop()
{
    ffmpeg->stop();
}

int SrsIngesterFFMPEG::cycle()
{
    return ffmpeg->cycle();
}

void SrsIngesterFFMPEG::fast_stop()
{
    ffmpeg->fast_stop();
}

SrsIngester::SrsIngester()
{
#if 1//def __SRS_DYNAMIC__
    channel_stream = 0;
    channel_file = 0;
#endif
    _srs_config->subscribe(this);
    
    pthread = new SrsReusableThread("ingest", this, SRS_AUTO_INGESTER_SLEEP_US);
    pprint = SrsPithyPrint::create_ingester();
}

SrsIngester::~SrsIngester()
{
    _srs_config->unsubscribe(this);
    
    srs_freep(pthread);
    clear_engines();
}

int SrsIngester::start()
{
    int ret = ERROR_SUCCESS;
    
    if ((ret = parse()) != ERROR_SUCCESS) {
        clear_engines();
        ret = ERROR_SUCCESS;
        return ret;
    }
    
    // even no ingesters, we must also start it,
    // for the reload may add more ingesters.
    
    // start thread to run all encoding engines.
    if ((ret = pthread->start()) != ERROR_SUCCESS) {
        srs_error("st_thread_create failed. ret=%d", ret);
        return ret;
    }
    srs_trace("ingest thread cid=%d, current_cid=%d", pthread->cid(), _srs_context->get_id());
    
    return ret;
}

int SrsIngester::parse_ingesters(SrsConfDirective* vhost)
{
    int ret = ERROR_SUCCESS;
    
    std::vector<SrsConfDirective*> ingesters = _srs_config->get_ingesters(vhost->arg0());
    
    // create engine
    for (int i = 0; i < (int)ingesters.size(); i++) {
        SrsConfDirective* ingest = ingesters[i];
        if ((ret = parse_engines(vhost, ingest)) != ERROR_SUCCESS) {
            return ret;
        }
    }
    
    return ret;
}

#if 1//def __SRS_DYNAMIC__
int SrsIngester::parse_engines(SrsConfDirective* vhost, SrsConfDirective* ingest)
{
    int ret = ERROR_SUCCESS;

    if (!_srs_config->get_ingest_enabled(ingest)) {
        return ret;
    }

    SrsIngestParam ingestPm;
    ingestPm.vhost = vhost->arg0();
    ingestPm.name = ingest->arg0();
    std::string input_type = _srs_config->get_ingest_input_type(ingest);
    if (srs_config_ingest_is_stream(input_type)) {
        ingestPm.input_type = 0x01;
    }
    else if (srs_config_ingest_is_file(input_type)) {
        ingestPm.input_type = 0x02;
    }
    else {
        ret = ERROR_ENCODER_INPUT_TYPE;
        srs_error("invalid ingest=%s type=%s, ret=%d",
            ingest->arg0().c_str(), input_type.c_str(), ret);
        return ret;
    }

    ingestPm.input = _srs_config->get_ingest_input_url(ingest);
    if (ingestPm.input.empty()) {
        ret = ERROR_ENCODER_NO_INPUT;
        srs_error("empty intput url, ingest=%s. ret=%d", ingest->arg0().c_str(), ret);
        return ret;
    }

    ingestPm.ffmpeg_bin = _srs_config->get_ingest_ffmpeg(ingest);
    if (ingestPm.ffmpeg_bin.empty()) {
        ret = ERROR_ENCODER_PARSE;
        srs_error("empty ffmpeg ret=%d", ret);
        return ret;
    }

    // get all engines.
    std::vector<SrsConfDirective*> engines = _srs_config->get_transcode_engines(ingest);
    if (engines.size() != 1) {
        ret = ERROR_ENCODER_NO_OUTPUT;
        srs_error("empty engines ret=%d", ret);
        return ret;
    }

    ingestPm.output = _srs_config->get_engine_output(engines[0]);
    if (ingestPm.ffmpeg_bin.empty()) {
        ret = ERROR_ENCODER_NO_OUTPUT;
        srs_error("empty output ret=%d", ret);
        return ret;
    }

    // get listen port
    std::string port;
    if (true) {
        std::vector<std::string> ip_ports = _srs_config->get_listens();
        srs_assert(ip_ports.size() > 0);

        std::string ep = ip_ports[0];
        std::string ip;
        srs_parse_endpoint(ep, ip, port);
    }

    ingestPm.output = srs_string_replace(ingestPm.output, "[port]", port);
    //ingestPm.output = srs_string_replace(ingestPm.output, "[channel]", str_channel);

    std::string str_key = ingestPm.vhost + "/" + ingestPm.name;
    ingesters_config[str_key] = ingestPm;
    srs_trace("ingest config add key: %s input: %s output: %s ffmpeg: %s", 
        str_key.c_str(), ingestPm.input.c_str(), ingestPm.output.c_str(), ingestPm.ffmpeg_bin.c_str());
    return ret;
}
#else
int SrsIngester::parse_engines(SrsConfDirective* vhost, SrsConfDirective* ingest)
{
    int ret = ERROR_SUCCESS;

    if (!_srs_config->get_ingest_enabled(ingest)) {
        return ret;
    }
    
    std::string ffmpeg_bin = _srs_config->get_ingest_ffmpeg(ingest);
    if (ffmpeg_bin.empty()) {
        ret = ERROR_ENCODER_PARSE;
        srs_trace("empty ffmpeg ret=%d", ret);
        return ret;
    }
    
    // get all engines.
    std::vector<SrsConfDirective*> engines = _srs_config->get_transcode_engines(ingest);
    
    // create ingesters without engines.
    if (engines.empty()) {
        SrsFFMPEG* ffmpeg = new SrsFFMPEG(ffmpeg_bin);
        if ((ret = initialize_ffmpeg(ffmpeg, vhost, ingest, NULL)) != ERROR_SUCCESS) {
            srs_freep(ffmpeg);
            if (ret != ERROR_ENCODER_LOOP) {
                srs_error("invalid ingest engine. ret=%d", ret);
            }
            return ret;
        }

        SrsIngesterFFMPEG* ingester = new SrsIngesterFFMPEG();
        if ((ret = ingester->initialize(ffmpeg, vhost->arg0(), ingest->arg0())) != ERROR_SUCCESS) {
            srs_freep(ingester);
            return ret;
        }
        
        ingesters.push_back(ingester);
        return ret;
    }

    // create ingesters with engine
    for (int i = 0; i < (int)engines.size(); i++) {
        SrsConfDirective* engine = engines[i];
        SrsFFMPEG* ffmpeg = new SrsFFMPEG(ffmpeg_bin);
        if ((ret = initialize_ffmpeg(ffmpeg, vhost, ingest, engine)) != ERROR_SUCCESS) {
            srs_freep(ffmpeg);
            if (ret != ERROR_ENCODER_LOOP) {
                srs_error("invalid ingest engine: %s %s, ret=%d", 
                    ingest->arg0().c_str(), engine->arg0().c_str(), ret);
            }
            return ret;
        }
        
        SrsIngesterFFMPEG* ingester = new SrsIngesterFFMPEG();
        if ((ret = ingester->initialize(ffmpeg, vhost->arg0(), ingest->arg0())) != ERROR_SUCCESS) {
            srs_freep(ingester);
            return ret;
        }

        ingesters.push_back(ingester);
    }
    
    return ret;
}
#endif

void SrsIngester::dispose()
{
    // first, use fast stop to notice all FFMPEG to quit gracefully.
#if 1//def __SRS_DYNAMIC__
    std::map<int, SrsIngesterFFMPEG*>::iterator it;
    for (it = ingesters.begin(); it != ingesters.end(); ++it) {
        SrsIngesterFFMPEG* ingester = it->second;
#else
    std::vector<SrsIngesterFFMPEG*>::iterator it;
    for (it = ingesters.begin(); it != ingesters.end(); ++it) {
        SrsIngesterFFMPEG* ingester = *it;
#endif
        ingester->fast_stop();
    }
    
    if (!ingesters.empty()) {
        srs_trace("fast stop all ingesters ok.");
    }
    
    // then, use stop to wait FFMPEG quit one by one and send SIGKILL if needed.
    stop();
}

#if 1//def __SRS_DYNAMIC__
#include <srs_rtmp_stack.hpp>
int SrsIngester::ingest_active(SrsRequest* req)
{
    int channel = atoi(req->stream.c_str());
    std::map<int, SrsIngesterFFMPEG*>::iterator it = ingesters.find(channel);
    if (it == ingesters.end()) {
        srs_error("ingest active error to find channel: %d", channel);
        return ERROR_USER_INGEST_NOT_FOUND;
    }

    srs_trace("ingest active channel: %d", channel);
    it->second->active(true);
    return ERROR_SUCCESS;
}

void SrsIngester::ingest_unactive(SrsRequest* req)
{
    int channel = atoi(req->stream.c_str());
    std::map<int, SrsIngesterFFMPEG*>::iterator it = ingesters.find(channel);
    if (it == ingesters.end()) {
        srs_error("ingest unactive error to find channel: %d", channel);
        return;
    }

    srs_trace("ingest unactive channel: %d", channel);
    it->second->active(false);
    it->second->stop();

    ingest_remove(channel);
}

int SrsIngester::ingest_add(std::string v, std::string i, struct SrsRequestParam* pm, std::string& url_out)
{
    if (i.empty() || v.empty() || !pm) {
        srs_error("ingest add error param vhost: %s app: %s", v.c_str(), i.c_str());
        return ERROR_USER_PARAM;
    }

    std::string str_key = v + "/" + i;
    std::map<std::string, SrsIngestParam>::iterator iter = ingesters_config.find(str_key);
    if (iter == ingesters_config.end()) {
        srs_error("ingest add error to find vhost: %s app: %s key: %s", v.c_str(), i.c_str(), str_key.c_str());
        return ERROR_USER_INGEST_NOT_FOUND;
    }

    SrsIngestParam& param = iter->second;
    std::string str_output = param.output;
    std::string str_input = param.input;
    if (0x02 == param.input_type) {
        if (pm->starttime.empty() || pm->endtime.empty()) {
            srs_error("ingest add error empty time vhost: %s app: %s key: %s", v.c_str(), i.c_str(), str_key.c_str());
            return ERROR_USER_PARAM_TIME;
        }

        str_input = srs_string_replace(str_input, "[starttime]", pm->starttime);
        str_input = srs_string_replace(str_input, "[endtime]", pm->endtime);
    }
    else {
        pm->starttime = "";
        pm->endtime = "";
    }

    // 2¨¦?¨°¨º?¡¤?¡ä??¨²
    std::map<int, SrsIngesterFFMPEG*>::iterator iter_ing;
    for (iter_ing = ingesters.begin(); iter_ing != ingesters.end(); ++iter_ing) {
        if (iter_ing->second->equals(v, i, *pm)) {
            url_out = iter_ing->second->out_url();
            srs_trace("ingest add already exist vhost: %s app: %s key: %s", v.c_str(), i.c_str(), str_key.c_str());
            return ERROR_SUCCESS;
        }
    }

    str_input = srs_string_replace(str_input, "[username]", pm->username);
    str_input = srs_string_replace(str_input, "[password]", pm->password);
    str_input = srs_string_replace(str_input, "[ip]", pm->ip);
    str_input = srs_string_replace(str_input, "[channel]", pm->channel);

    int channel_id = -1;
    unsigned short& channel = (1 == param.input_type) ? channel_stream : channel_file;
    while (channel_id < 0) {
        channel_id = (int)(++channel) | ((1 == param.input_type) ? 0 : 0x00010000);
        if (ingesters.find(channel_id) != ingesters.end()) {
            channel_id = -1;
        }
    }

    char str_channel[32] = { 0 };
    sprintf(str_channel, "%d", channel_id);
    str_output = srs_string_replace(str_output, "[channel]", str_channel);
    url_out = str_output;

    // ¡ä¡ä?¡§ffmpeg
    SrsFFMPEG* ffmpeg = new SrsFFMPEG(param.ffmpeg_bin);
    SrsIngesterFFMPEG* ingester = new SrsIngesterFFMPEG();
    if (!ffmpeg || !ingester) {
        srs_freep(ffmpeg);
        srs_freep(ingester);
        srs_error("ingest add error alloc memory vhost: %s app: %s key: %s id: %d", v.c_str(), i.c_str(), str_key.c_str(), channel_id);
        return ERROR_USER_ALLOC_MEMORY;
    }

    std::string log_file = SRS_CONSTS_NULL_FILE; // disabled // write ffmpeg info to log file.
    if (_srs_config->get_ffmpeg_log_enabled()) {
        log_file = _srs_config->get_ffmpeg_log_dir();
        log_file += "/";
        log_file += "ffmpeg-ingest";
        log_file += "-";
        log_file += param.vhost;
        log_file += "-";
        log_file += param.name;
        log_file += "-";
        log_file += str_channel;
        log_file += ".log";
    }

    str_input = "rtsp://184.72.239.149/vod/mp4://BigBuckBunny_175k.mov";
    ffmpeg->initialize(str_input, str_output, log_file);
    ffmpeg->set_oformat("flv");
    ffmpeg->set_iparams("");
    ffmpeg->initialize_copy();

    int ret = ingester->initialize(ffmpeg, v, i, *pm, channel_id);
    if (ERROR_SUCCESS != ret) {
        srs_error("ingest add error to initialize ffmpeg vhost: %s app: %s key: %s id: %d", v.c_str(), i.c_str(), str_key.c_str(), channel_id);
        srs_freep(ffmpeg);
        srs_freep(ingester);
        return ret;
    }

    srs_trace("ingest add successful vhost: %s app: %s key: %s id: %d", v.c_str(), i.c_str(), str_key.c_str(), channel_id);
    ingesters.insert(std::make_pair(channel_id, ingester));
    return ret;
}

bool SrsIngester::identify_ingest(int id)
{
    return (ingesters.find(id) != ingesters.end());
}

void SrsIngester::ingest_remove(int id)
{
    if (id < 0) {
        return;
    }

    if ((id & 0xFFFF0000) == 0) {
        return;
    }

    std::map<int, SrsIngesterFFMPEG*>::iterator iter = ingesters.find(id);
    if (iter != ingesters.end()) {
        srs_trace("ingest remove id: %d", id);
        srs_freep(iter->second);
        ingesters.erase(iter);
    }
}
#endif

void SrsIngester::stop()
{
    pthread->stop();
    clear_engines();
}

int SrsIngester::cycle()
{
    int ret = ERROR_SUCCESS;
    
#if 1//def __SRS_DYNAMIC__
    std::map<int, SrsIngesterFFMPEG*>::iterator it;
    for (it = ingesters.begin(); it != ingesters.end(); ++it) {
        SrsIngesterFFMPEG* ingester = it->second;
#else
    std::vector<SrsIngesterFFMPEG*>::iterator it;
    for (it = ingesters.begin(); it != ingesters.end(); ++it) {
        SrsIngesterFFMPEG* ingester = *it;
#endif
        if (!ingester->active()) {
            continue;
        }
        
        // start all ffmpegs.
        if ((ret = ingester->start()) != ERROR_SUCCESS) {
            srs_error("ingest ffmpeg start failed. ret=%d", ret);
            return ret;
        }

        // check ffmpeg status.
        if ((ret = ingester->cycle()) != ERROR_SUCCESS) {
            srs_error("ingest ffmpeg cycle failed. ret=%d", ret);
            return ret;
        }
    }

    // pithy print
    show_ingest_log_message();
    
    return ret;
}

void SrsIngester::on_thread_stop()
{
}

void SrsIngester::clear_engines()
{
#if 1//def __SRS_DYNAMIC__
    std::map<int, SrsIngesterFFMPEG*>::iterator it;

    for (it = ingesters.begin(); it != ingesters.end(); ++it) {
        SrsIngesterFFMPEG* ingester = it->second;
#else
    std::vector<SrsIngesterFFMPEG*>::iterator it;
    
    for (it = ingesters.begin(); it != ingesters.end(); ++it) {
        SrsIngesterFFMPEG* ingester = *it;
#endif
        srs_freep(ingester);
    }

    ingesters.clear();
}

int SrsIngester::parse()
{
    int ret = ERROR_SUCCESS;
    
    // parse ingesters
    std::vector<SrsConfDirective*> vhosts;
    _srs_config->get_vhosts(vhosts);
    
    for (int i = 0; i < (int)vhosts.size(); i++) {
        SrsConfDirective* vhost = vhosts[i];
        if ((ret = parse_ingesters(vhost)) != ERROR_SUCCESS) {
            return ret;
        }
    }
    
    return ret;
}

int SrsIngester::initialize_ffmpeg(SrsFFMPEG* ffmpeg, SrsConfDirective* vhost, SrsConfDirective* ingest, SrsConfDirective* engine)
{
    int ret = ERROR_SUCCESS;
    
    std::string port;
    if (true) {
        std::vector<std::string> ip_ports = _srs_config->get_listens();
        srs_assert(ip_ports.size() > 0);
        
        std::string ep = ip_ports[0];
        std::string ip;
        srs_parse_endpoint(ep, ip, port);
    }
    
    std::string output = _srs_config->get_engine_output(engine);
    // output stream, to other/self server
    // ie. rtmp://localhost:1935/live/livestream_sd
    output = srs_string_replace(output, "[vhost]", vhost->arg0());
    output = srs_string_replace(output, "[port]", port);
    if (output.empty()) {
        ret = ERROR_ENCODER_NO_OUTPUT;
        srs_trace("empty output url, ingest=%s. ret=%d", ingest->arg0().c_str(), ret);
        return ret;
    }
    
    // find the app and stream in rtmp url
    std::string url = output;
    std::string app, stream;
    size_t pos = std::string::npos;
    if ((pos = url.rfind("/")) != std::string::npos) {
        stream = url.substr(pos + 1);
        url = url.substr(0, pos);
    }
    if ((pos = url.rfind("/")) != std::string::npos) {
        app = url.substr(pos + 1);
        url = url.substr(0, pos);
    }
    if ((pos = app.rfind("?")) != std::string::npos) {
        app = app.substr(0, pos);
    }
    
    std::string log_file = SRS_CONSTS_NULL_FILE; // disabled
    // write ffmpeg info to log file.
    if (_srs_config->get_ffmpeg_log_enabled()) {
        log_file = _srs_config->get_ffmpeg_log_dir();
        log_file += "/";
        log_file += "ffmpeg-ingest";
        log_file += "-";
        log_file += vhost->arg0();
        log_file += "-";
        log_file += app;
        log_file += "-";
        log_file += stream;
        log_file += ".log";
    }
    
    // input
    std::string input_type = _srs_config->get_ingest_input_type(ingest);
    if (input_type.empty()) {
        ret = ERROR_ENCODER_NO_INPUT;
        srs_trace("empty intput type, ingest=%s. ret=%d", ingest->arg0().c_str(), ret);
        return ret;
    }

    if (srs_config_ingest_is_file(input_type)) {
        std::string input_url = _srs_config->get_ingest_input_url(ingest);
        if (input_url.empty()) {
            ret = ERROR_ENCODER_NO_INPUT;
            srs_trace("empty intput url, ingest=%s. ret=%d", ingest->arg0().c_str(), ret);
            return ret;
        }
        
        // for file, set re.
        ffmpeg->set_iparams("-re");
    
        if ((ret = ffmpeg->initialize(input_url, output, log_file)) != ERROR_SUCCESS) {
            return ret;
        }
    } else if (srs_config_ingest_is_stream(input_type)) {
        std::string input_url = _srs_config->get_ingest_input_url(ingest);
        if (input_url.empty()) {
            ret = ERROR_ENCODER_NO_INPUT;
            srs_trace("empty intput url, ingest=%s. ret=%d", ingest->arg0().c_str(), ret);
            return ret;
        }
        
        // for stream, no re.
        ffmpeg->set_iparams("");
    
        if ((ret = ffmpeg->initialize(input_url, output, log_file)) != ERROR_SUCCESS) {
            return ret;
        }
    } else {
        ret = ERROR_ENCODER_INPUT_TYPE;
        srs_error("invalid ingest=%s type=%s, ret=%d", 
            ingest->arg0().c_str(), input_type.c_str(), ret);
    }
    
    // set output format to flv for RTMP
    ffmpeg->set_oformat("flv");
    
    std::string vcodec = _srs_config->get_engine_vcodec(engine);
    std::string acodec = _srs_config->get_engine_acodec(engine);
    // whatever the engine config, use copy as default.
    bool engine_disabled = !engine || !_srs_config->get_engine_enabled(engine);
    if (engine_disabled || vcodec.empty() || acodec.empty()) {
        if ((ret = ffmpeg->initialize_copy()) != ERROR_SUCCESS) {
            return ret;
        }
    } else {
        if ((ret = ffmpeg->initialize_transcode(engine)) != ERROR_SUCCESS) {
            return ret;
        }
    }
    
    srs_trace("parse success, ingest=%s, vhost=%s", 
        ingest->arg0().c_str(), vhost->arg0().c_str());
    
    return ret;
}

void SrsIngester::show_ingest_log_message()
{
    pprint->elapse();

    if ((int)ingesters.size() <= 0) {
        return;
    }
    
    // random choose one ingester to report.
    int index = rand() % (int)ingesters.size();
#if 1//def __SRS_DYNAMIC__
    SrsIngesterFFMPEG* ingester = NULL;
    std::map<int, SrsIngesterFFMPEG*>::iterator iter;
    for (iter = ingesters.begin(); iter != ingesters.end(); ++iter) {
        if (0 == index) {
            ingester = iter->second;
            break;
        }
        
        --index;
    }
    
    if (!ingester) {
        return;
    }
#else
    SrsIngesterFFMPEG* ingester = ingesters.at(index);
#endif
    
    // reportable
    if (pprint->can_print()) {
        srs_trace("-> "SRS_CONSTS_LOG_INGESTER" time=%"PRId64", ingesters=%d, #%d(alive=%ds, %s)",
            pprint->age(), (int)ingesters.size(), index, ingester->alive() / 1000, ingester->uri().c_str());
    }
}

int SrsIngester::on_reload_vhost_added(string vhost)
{
    int ret = ERROR_SUCCESS;
    
    SrsConfDirective* _vhost = _srs_config->get_vhost(vhost);
    if ((ret = parse_ingesters(_vhost)) != ERROR_SUCCESS) {
        return ret;
    }
    
    srs_trace("reload add vhost ingesters, vhost=%s", vhost.c_str());

    return ret;
}

int SrsIngester::on_reload_vhost_removed(string vhost)
{
    int ret = ERROR_SUCCESS;
    
#if 1//def __SRS_DYNAMIC__
    std::map<int, SrsIngesterFFMPEG*>::iterator it;
    std::vector<int> ary_remove;

    for (it = ingesters.begin(); it != ingesters.end();) {
        SrsIngesterFFMPEG* ingester = it->second;
#else
    std::vector<SrsIngesterFFMPEG*>::iterator it;
    
    for (it = ingesters.begin(); it != ingesters.end();) {
        SrsIngesterFFMPEG* ingester = *it;
#endif
        
        if (!ingester->equals(vhost)) {
            ++it;
            continue;
        }
        
        // stop the ffmpeg and free it.
        ingester->stop();
        
        srs_trace("reload stop ingester, vhost=%s, id=%s", vhost.c_str(), ingester->uri().c_str());
            
        srs_freep(ingester);
        
        // remove the item from ingesters.
#if 1//def __SRS_DYNAMIC__
        ary_remove.push_back(it->first);
#else
        it = ingesters.erase(it);
#endif
    }

#if 1//def __SRS_DYNAMIC__
    std::vector<int>::iterator iter;
    for (iter = ary_remove.begin(); iter != ary_remove.end(); ++iter) {
        ingesters.erase(*iter);
    }
#endif

    return ret;
}

int SrsIngester::on_reload_ingest_removed(string vhost, string ingest_id)
{
    int ret = ERROR_SUCCESS;
    
#if 1//def __SRS_DYNAMIC__
    std::map<int, SrsIngesterFFMPEG*>::iterator it;
    std::vector<int> ary_remove;

    for (it = ingesters.begin(); it != ingesters.end();) {
        SrsIngesterFFMPEG* ingester = it->second;
#else
    std::vector<SrsIngesterFFMPEG*>::iterator it;
    
    for (it = ingesters.begin(); it != ingesters.end();) {
        SrsIngesterFFMPEG* ingester = *it;
#endif
        
        if (!ingester->equals(vhost, ingest_id)) {
            ++it;
            continue;
        }
        
        // stop the ffmpeg and free it.
        ingester->stop();
        
        srs_trace("reload stop ingester, vhost=%s, id=%s", vhost.c_str(), ingester->uri().c_str());
            
        srs_freep(ingester);
        
        // remove the item from ingesters.
#if 1//def __SRS_DYNAMIC__
        ary_remove.push_back(it->first);
#else
        it = ingesters.erase(it);
#endif
    }

#if 1//def __SRS_DYNAMIC__
    std::vector<int>::iterator iter;
    for (iter = ary_remove.begin(); iter != ary_remove.end(); ++iter) {
        ingesters.erase(*iter);
    }
#endif
    
    return ret;
}

int SrsIngester::on_reload_ingest_added(string vhost, string ingest_id)
{
    int ret = ERROR_SUCCESS;
    
    SrsConfDirective* _vhost = _srs_config->get_vhost(vhost);
    SrsConfDirective* _ingester = _srs_config->get_ingest_by_id(vhost, ingest_id);
    
    if ((ret = parse_engines(_vhost, _ingester)) != ERROR_SUCCESS) {
        return ret;
    }
    
    srs_trace("reload add ingester, "
        "vhost=%s, id=%s", vhost.c_str(), ingest_id.c_str());
    
    return ret;
}

int SrsIngester::on_reload_ingest_updated(string vhost, string ingest_id)
{
    int ret = ERROR_SUCCESS;

    if ((ret = on_reload_ingest_removed(vhost, ingest_id)) != ERROR_SUCCESS) {
        return ret;
    }

    if ((ret = on_reload_ingest_added(vhost, ingest_id)) != ERROR_SUCCESS) {
        return ret;
    }
    
    srs_trace("reload updated ingester, "
        "vhost=%s, id=%s", vhost.c_str(), ingest_id.c_str());
    
    return ret;
}

#endif

