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

#ifdef __INGEST_DYNAMIC__
#include <srs_app_source.hpp>
#include <srs_rtmp_utility.hpp>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#endif

// when error, ingester sleep for a while and retry.
// ingest never sleep a long time, for we must start the stream ASAP.
#define SRS_AUTO_INGESTER_SLEEP_US (int64_t)(3*1000*1000LL)

SrsIngesterFFMPEG::SrsIngesterFFMPEG()
{
#ifdef __INGEST_DYNAMIC__
    time_leave_ = 180;
    need_remove_ = false;
    ff_active = false;
    n_channel = -1;
    tm_update_ = 0;
#endif
    ffmpeg = NULL;
}

SrsIngesterFFMPEG::~SrsIngesterFFMPEG()
{
    srs_freep(ffmpeg);
}

#ifdef __INGEST_DYNAMIC__
std::string SrsIngesterFFMPEG::channel_out()
{
    return channel_out_;
}

void SrsIngesterFFMPEG::active(bool enable, time_t tm_update /*= 0*/)
{
    if (!enable) {
        ffmpeg->reconnect_count_reset();
        if (n_channel > 0 && (n_channel & 0xFFFF0000) > 0) {
            need_remove_ = true;
        }
    }

    if (tm_update > 0) {
        tm_update_ = tm_update;
        ff_active = enable ? (ff_active | 0x01) : (ff_active & ((unsigned char)(~0x01)));
    }
    else {
        ff_active = enable ? (ff_active | 0x02) : (ff_active & ((unsigned char)(~0x02)));
    }
}

bool SrsIngesterFFMPEG::need_remove()
{
    return need_remove_;
}

bool SrsIngesterFFMPEG::active()
{
    return ff_active;
}

int SrsIngesterFFMPEG::initialize(SrsFFMPEG* ff, std::string v, std::string i, std::string ip, std::string ch_in, std::string ch_out)
{
    int ret = ERROR_SUCCESS;

    ip_in_ = ip;
    channel_in_ = ch_in;
    channel_out_ = ch_out;
    n_channel = atoi(channel_out_.c_str());
    time_leave_ = _srs_config->get_hls_leave_time(v);
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

#ifdef __INGEST_DYNAMIC__
bool SrsIngesterFFMPEG::equals(std::string v, std::string i, std::string ip, std::string ch_in)
{
    return vhost == v && id == i && (ip_in_ == ip) && (channel_in_ == ch_in);
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
#ifdef __INGEST_DYNAMIC__
    if (ffmpeg->is_running()) {
        return ERROR_SUCCESS;
    }

    if (!(ffmpeg->need_reconnect())) {
        std::string str_key = srs_generate_stream_url(vhost, "live", channel_out_);
        SrsSource::close_source_client(str_key);
        active(false);

        return ERROR_SUCCESS;
    }
#endif
    return ffmpeg->start();
}

void SrsIngesterFFMPEG::stop()
{
    bool b_ret = ffmpeg->is_running();
    ffmpeg->stop();

#ifdef __INGEST_DYNAMIC__
    if (b_ret) {
        DIR *dirp = NULL;
        dirent *dp = NULL;
        struct stat dir_stat;

        char cur_dir[] = ".";
        char up_dir[] = "..";

        std::string full_path;
        std::string str_path = _srs_config->get_hls_path(vhost) + "/live/";

        if (0 == access(str_path.c_str(), F_OK) &&
            0 == stat(str_path.c_str(), &dir_stat) &&
            S_ISDIR(dir_stat.st_mode)) {
            dirp = opendir(str_path.c_str());
            while ((dp = readdir(dirp)) != NULL) {
                if ((0 == strcmp(cur_dir, dp->d_name)) || (0 == strcmp(up_dir, dp->d_name))) {
                    continue;
                }

                if (0 != strncmp(channel_out_.c_str(), dp->d_name, channel_out_.length())) {
                    continue;
                }

                full_path = str_path + dp->d_name;
                remove(full_path.c_str());
            }
            closedir(dirp);
        }
    }
#endif
}

int SrsIngesterFFMPEG::cycle()
{
#ifdef __INGEST_DYNAMIC__
    if (tm_update_ > 0 && (ff_active & 0x01)) {
        if (time(NULL) > tm_update_ + time_leave_) {
            ff_active = (ff_active & ((unsigned char)(~0x01)));
        }
    }
#endif

    return ffmpeg->cycle();
}

void SrsIngesterFFMPEG::fast_stop()
{
    ffmpeg->fast_stop();
}

SrsIngester::SrsIngester()
{
#ifdef __INGEST_DYNAMIC__
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

#ifdef __INGEST_DYNAMIC__
int SrsIngester::parse_engines(SrsConfDirective* vhost, SrsConfDirective* ingest)
{
    int ret = ERROR_SUCCESS;

    if (!_srs_config->get_ingest_enabled(ingest)) {
        return ret;
    }

    SrsIngestParam ingestPm;
    ingestPm.vhost = vhost->arg0();
    ingestPm.ingesttype = ingest->arg0();
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

    // get all engines.
    std::vector<SrsConfDirective*> engines = _srs_config->get_transcode_engines(ingest);
    if (engines.size() != 1) {
        ret = ERROR_ENCODER_NO_OUTPUT;
        srs_error("empty engines ret=%d", ret);
        return ret;
    }

    ingestPm.output = _srs_config->get_engine_output(engines[0]);
    if (ingestPm.output.empty()) {
        ret = ERROR_ENCODER_NO_OUTPUT;
        srs_error("empty output ret=%d", ret);
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

    bool b_enable = _srs_config->get_engine_enabled(engines[0]);
    std::string vcodec = _srs_config->get_engine_vcodec(engines[0]);
    std::string acodec = _srs_config->get_engine_acodec(engines[0]);
	if (b_enable && !vcodec.empty() && !acodec.empty()) {
        ingestPm.engine = engines[0];
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

    std::string str_key = ingestPm.get_stream_url();
    srs_trace("ingest config add key: %s input: %s output: %s ffmpeg: %s", 
        str_key.c_str(), ingestPm.input.c_str(), ingestPm.output.c_str(), ingestPm.ffmpeg_bin.c_str());
    ingesters_config[str_key] = ingestPm;
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
#ifdef __INGEST_DYNAMIC__
    map_ingesters::iterator it;
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

#ifdef __INGEST_DYNAMIC__
#include <srs_rtmp_stack.hpp>
long SrsIngester::ingest_active(SrsRequest* req, time_t tm_update)
{
    map_ingesters::iterator it = ingesters.find(req->get_stream_url());
    if (it == ingesters.end()) {
        srs_error("ingest: %s active error to find", req->get_stream_url().c_str());
        return ERROR_USER_INGEST_NOT_FOUND;
    }

    srs_trace("ingest: %s actived.", req->get_stream_url().c_str());
    it->second->active(true, tm_update);
    return ERROR_SUCCESS;
}

bool SrsIngester::ingest_unactive(SrsRequest* req)
{
    map_ingesters::iterator it = ingesters.find(req->get_stream_url());
    if (it == ingesters.end()) {
        srs_error("ingest: %s unactive error to find", req->get_stream_url().c_str());
        return false;
    }

    srs_trace("ingest: %s unactived", req->get_stream_url().c_str());
    it->second->active(false);
    return true;
}

long SrsIngester::ingest_add(struct SrsRequestParam* pm, std::string& out_channel)
{
    if (!pm) {
        srs_error("ingest add error param.");
        return ERROR_USER_PARAM;
    }

    std::string str_key = pm->get_stream_url();
    map_ingesters_config::iterator iter = ingesters_config.find(str_key);
    if (iter == ingesters_config.end()) {
        srs_error("ingest add error to find key: %s", str_key.c_str());
        return ERROR_USER_HAS_NO_CONFIG;
    }

    SrsIngestParam& param = iter->second;    
    std::string str_output = param.output;
    std::string str_input = param.input;

    if (0x02 == param.input_type) {
        // 回播
        if (pm->starttime.empty() || pm->endtime.empty()) {
            srs_error("ingest add error empty time key: %s", str_key.c_str());
            return ERROR_USER_PARAM_TIME;
        }

        str_input = srs_string_replace(str_input, "[starttime]", pm->starttime);
        str_input = srs_string_replace(str_input, "[endtime]", pm->endtime);
    }
    else {
        // 直播
        pm->starttime = "";
        pm->endtime = "";

        map_ingesters::iterator iter_ing;
        for (iter_ing = ingesters.begin(); iter_ing != ingesters.end(); ++iter_ing) {
            if (iter_ing->second->equals(pm->vhost, pm->ingesttype, pm->ip, pm->channel)) {
                out_channel = iter_ing->second->channel_out();
                srs_trace("ingest add already exist key: %s", str_key.c_str());
                return ERROR_SUCCESS;
            }
        }
    }

    str_input = srs_string_replace(str_input, "[username]", pm->username);
    str_input = srs_string_replace(str_input, "[password]", pm->password);
    str_input = srs_string_replace(str_input, "[ip]", pm->ip);
    str_input = srs_string_replace(str_input, "[channel]", pm->channel);

    int channel_id = -1;
    char sz_channel[32] = { 0 };
    unsigned short& channel_use = (1 == param.input_type) ? channel_stream : channel_file;
    while (channel_id < 0) {
        channel_id = (int)(++channel_use) | ((1 == param.input_type) ? 0 : 0x00010000);
        if (0 == channel_use) {
            channel_id = -1;
			continue;
        }

        sprintf(sz_channel, "%d", channel_id);
        if (ingesters.find(sz_channel) != ingesters.end()) {
            channel_id = -1;
        }
    }

    str_output = srs_string_replace(str_output, "[channel]", sz_channel);
    out_channel = sz_channel;

    // 创建 ffmpeg
    SrsFFMPEG* ffmpeg = new SrsFFMPEG(param.ffmpeg_bin, (0x02 == param.input_type) ? 1 : 3);
    SrsIngesterFFMPEG* ingester = new SrsIngesterFFMPEG();
    if (!ffmpeg || !ingester) {
        srs_freep(ffmpeg);
        srs_freep(ingester);
        srs_error("ingest add error alloc memory key: %s id: %d", str_key.c_str(), channel_id);
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
        log_file += param.ingesttype;
        log_file += "-";
        log_file += sz_channel;
        log_file += ".log";
    }

    ffmpeg->initialize(str_input, str_output, log_file);
    ffmpeg->set_oformat("flv");
    ffmpeg->set_iparams("");

    if (param.engine) {
        if (ERROR_SUCCESS != ffmpeg->initialize_transcode(param.engine)) {
            return ERROR_USER_FFMPEG_INITIALIZE;
        }
    }
    else {
        if (ERROR_SUCCESS != ffmpeg->initialize_copy()) {
            return ERROR_USER_FFMPEG_INITIALIZE;
        }
    }

    int ret = ingester->initialize(ffmpeg, pm->vhost, pm->ingesttype, pm->ip, pm->channel, out_channel);
    if (ERROR_SUCCESS != ret) {
        srs_error("ingest add error to initialize ffmpeg key: %s id: %d", str_key.c_str(), channel_id);
        srs_freep(ffmpeg);
        srs_freep(ingester);
        return ERROR_USER_INGESTER_INITIALIZE;
    }

    srs_trace("ingest add successful key: %s id: %d", str_key.c_str(), channel_id);
    ingesters.insert(std::make_pair(sz_channel, ingester));
    return ERROR_SUCCESS;
}

bool SrsIngester::ingest_identify(SrsRequest* req)
{
    map_ingesters::iterator iter = ingesters.find(req->get_stream_url());
	if (iter == ingesters.end() || NULL == iter->second) {
        return false;
	}

    return !(iter->second->need_remove());
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
    
#ifdef __INGEST_DYNAMIC__
    map_ingesters::iterator it;
    std::vector<map_ingesters::iterator> ary_erase;
    for (it = ingesters.begin(); it != ingesters.end(); ++it) {
        SrsIngesterFFMPEG* ingester = it->second;
        if (!ingester) {
            continue;
        }

        if (!ingester->active()) {
            ingester->stop();

            if (ingester->need_remove()) {
                ary_erase.push_back(it);
            }
            continue;
        }
#else
    std::vector<SrsIngesterFFMPEG*>::iterator it;
    for (it = ingesters.begin(); it != ingesters.end(); ++it) {
        SrsIngesterFFMPEG* ingester = *it;
#endif
        
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

#ifdef __INGEST_DYNAMIC__
    std::vector<map_ingesters::iterator>::iterator iter = ary_erase.begin();
    for (; iter != ary_erase.end(); ++iter) {
        delete ((*iter)->second);

        ingesters.erase(*iter);
	}
#endif

    // pithy print
    show_ingest_log_message();
    
    return ret;
}

void SrsIngester::on_thread_stop()
{
}

void SrsIngester::clear_engines()
{
#ifdef __INGEST_DYNAMIC__
    map_ingesters::iterator it;

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
#ifdef __INGEST_DYNAMIC__
    SrsIngesterFFMPEG* ingester = NULL;
    map_ingesters::iterator iter;
    for (iter = ingesters.begin(); iter != ingesters.end(); ++iter) {
        if (0 == index) {
            ingester = iter->second;
            break;
        }
        
        --index;
    }
    
    if (!ingester || !ingester->active()) {
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
    
#ifdef __INGEST_DYNAMIC__
    map_ingesters::iterator it;
    std::vector<std::string> ary_remove;

    for (it = ingesters.begin(); it != ingesters.end();) {
        SrsIngesterFFMPEG* ingester = it->second;
        it->second = NULL;
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
#ifdef __INGEST_DYNAMIC__
        ary_remove.push_back(it->first);
#else
        it = ingesters.erase(it);
#endif
    }

#ifdef __INGEST_DYNAMIC__
    std::vector<std::string>::iterator iter;
    for (iter = ary_remove.begin(); iter != ary_remove.end(); ++iter) {
        ingesters.erase(*iter);
    }
#endif

    return ret;
}

int SrsIngester::on_reload_ingest_removed(string vhost, string ingest_id)
{
    int ret = ERROR_SUCCESS;
    
#ifdef __INGEST_DYNAMIC__
    map_ingesters::iterator it;
    std::vector<std::string> ary_remove;

    for (it = ingesters.begin(); it != ingesters.end();) {
        SrsIngesterFFMPEG* ingester = it->second;
        it->second = NULL;
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
#ifdef __INGEST_DYNAMIC__
        ary_remove.push_back(it->first);
#else
        it = ingesters.erase(it);
#endif
    }

#ifdef __INGEST_DYNAMIC__
    std::vector<std::string>::iterator iter;
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

