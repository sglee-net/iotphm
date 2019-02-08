// daq.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include "pch.h"

#include <stdio.h>
#include <stdlib.h>
#include <NIDAQmx.h>
#include <time.h>
#include "./glogger/glogger.h"
#include "./gexecutor/gexecutor_filewriter.h"
#include "./gtaskque/gtaskque.h"
#include "./gexecutor/gexecutor_thrift_writemessage.h"
#include <thread>
#include <sstream>
#include <chrono>
#include <ctime>
#include <iostream>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/posix_time/posix_time_io.hpp>
#include <boost/lexical_cast.hpp>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/stdcxx.h>
#include "./messenger_constants.h"
#include "./messenger_types.h"
#include "./ThriftRWService.h"
#include <crtdbg.h>
#include <direct.h>

#define DAQmxErrChk(functionCall) if( DAQmxFailed(error=(functionCall)) ) goto Error; else

#define DONT_EXPAND_MACRO

int32 CVICALLBACK EveryNCallback(TaskHandle taskHandle, int32 everyNsamplesEventType, uInt32 nSamples, void *callbackData);
int32 CVICALLBACK DoneCallback(TaskHandle taskHandle, int32 status, void *callbackData);

#define nCHANNEL 4
#define nSAMPLES 10000
#define nSIZE nCHANNEL*nSAMPLES
#define maxFilePathLength 256

using namespace std::chrono;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace thrift_gen;

void getCurrentDirectory(string &_path) {
	char *strBuffer = new char[maxFilePathLength];
	char *rt = nullptr;
#ifdef WIN32
	rt = _getcwd(strBuffer, maxFilePathLength);
#elif __linux__
	rt = getcwd(strBuffer, maxFilePathLength);
#endif

	if (rt) {
		_path = rt;
	}

	delete strBuffer;
}

int main(void)
{

	std::chrono::system_clock::time_point start_time = std::chrono::system_clock::now();
	
	string current_path;
	::getCurrentDirectory(current_path);
	string accel_file = current_path + "\\accel.csv";
	string logger_file = current_path + "\\log.txt";

	ofstream *fileout_accel = new ofstream(accel_file, ios::out);
	GLogger<string, ofstream> *file_writer =
		GLogger<string, ofstream>::getInstance();
	GExecutorInterface<string, ofstream> *appender_accel =
		new GExecutorFileWriter<string, ofstream>(fileout_accel, true);
	file_writer->addAppender("filewriter", appender_accel);
	cout << "filewriter is defined" << endl;

	//	ofstream *fileout_logger = new ofstream(logger_file, ios::out);
	//	GLogger<string, ofstream> *logger = new GLogger<string, ofstream>();
	////		GLogger<string, ofstream>::getInstance();
	//	GExecutorInterface<string, ofstream> *appender_logger =
	//		new GExecutorFileWriter<string, ofstream>(fileout_logger, true);
	//	file_writer->addAppender("logger", appender_logger);

	stdcxx::shared_ptr<TTransport> socket(new TSocket("192.168.0.13", 9091)); // 106.243.132.138 192.168.0.41
	stdcxx::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	stdcxx::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	ThriftRWServiceClient client(protocol);
	ThriftRWServiceClient *thriftSerivce = &client;

	const size_t buffer_size = 10;
	GExecutorInterface<ThriftMessage, ThriftRWServiceClient> *executor =
		new GExecutorThriftWriteMessage<ThriftMessage, ThriftRWServiceClient>(thriftSerivce, false);
	GTaskQue<ThriftMessage, ThriftRWServiceClient> *taskque =
		new GTaskQue<ThriftMessage, ThriftRWServiceClient>(executor, buffer_size);

	try {
		transport->open();
		if (!transport->isOpen()) {
			//			logger->error(__FILE__, __LINE__, "thrift port is not opened");
			cerr << "thrift port is not opened";
			exit(1);
		}

		//////client.setMsg(msg);
		//for (int32_t i = 0; i<1000; i++) {
		//	cout << i << endl;
		//	string str = "data" + std::to_string(i);
		//	Message msg;
		//	msg._payload = str;
		//	string rt;
		//	try {
		//		client.writeMessage(rt, msg);
		//	}
		//	catch (...) {

		//	}
		//	cout << rt << endl;
		//	//client.writeI32(i);
		//	//logger->debug(__FILE__, __LINE__, "writeI32(%d)", i);
		//	//usleep(1000);
		//}

		//transport->close();

	}
	catch (TException& tx) {
		cout << "ERROR: " << tx.what() << endl;
		//		logger->error(__FILE__, __LINE__, tx.what());
		cerr << tx.what();
		//if (logger) {
		//	delete logger;
		//}
		exit(1);
	}

	//return 0;

	taskque->doAutoExecution(true);

	int32       error = 0;
	TaskHandle  taskHandle = 0;
	char        errBuff[2048] = { '\0' };

	/*********************************************/
	// DAQmx Configure Code
	/*********************************************/
	DAQmxErrChk(DAQmxCreateTask("AccelTask", &taskHandle));
	printf("1\n");
	DAQmxErrChk(DAQmxCreateAIAccelChan(taskHandle, "cDAQ1Mod1/ai0:3", "", DAQmx_Val_PseudoDiff, -100.0, 100.0, DAQmx_Val_AccelUnit_g, 50, DAQmx_Val_mVoltsPerG, DAQmx_Val_Internal, 0.004, NULL));
	printf("2\n");
	// rate 10K
	DAQmxErrChk(DAQmxCfgSampClkTiming(taskHandle, "", 10000.0, DAQmx_Val_Rising, DAQmx_Val_ContSamps, nSAMPLES));
	printf("3\n");
	//		DAQmxErrChk (DAQmxCfgAnlgEdgeStartTrig(taskHandle,"cDAQ1Mod1/ai1",DAQmx_Val_Rising,0.1));
	printf("4\n");
	//		DAQmxErrChk (DAQmxSetAnlgEdgeStartTrigHyst(taskHandle, 4.0));
	printf("5\n");

	DAQmxErrChk(DAQmxRegisterEveryNSamplesEvent(taskHandle, DAQmx_Val_Acquired_Into_Buffer, nSAMPLES, 0, EveryNCallback, taskque));
	printf("6\n");
	DAQmxErrChk(DAQmxRegisterDoneEvent(taskHandle, 0, DoneCallback, NULL));
	printf("7\n");

	/*********************************************/
	// DAQmx Start Code
	/*********************************************/
	DAQmxErrChk(DAQmxStartTask(taskHandle));
	printf("8\n");

	printf("Acquiring samples continuously. Press Enter to interrupt\n");
	getchar();

Error:
	if (DAQmxFailed(error))
		DAQmxGetExtendedErrorInfo(errBuff, 2048);
	if (taskHandle != 0) {
		/*********************************************/
		// DAQmx Stop Code
		/*********************************************/
		DAQmxStopTask(taskHandle);
		DAQmxClearTask(taskHandle);
	}
	if (DAQmxFailed(error))
		printf("DAQmx Error: %s\n", errBuff);

	while (taskque->isRunning()) {
		Sleep(1000);
		if (taskque->areAllTasksExecuted()) {
			break;
		}
	}
	if (taskque) {
		delete taskque;
	}


	printf("End of program, press Enter key to quit\n");
	transport->close();
	printf("Thrift is closed\n");
	//	logger->info(__FILE__, __LINE__, "Thrift is closed");

	getchar();
	//if (logger) {
	//	delete logger;
	//}
	return 0;
}
static int ggg = 0;

int32 CVICALLBACK EveryNCallback(TaskHandle taskHandle, int32 everyNsamplesEventType, uInt32 nSamples, void *callbackData)
{
	int32       error = 0;
	char        errBuff[2048] = { '\0' };
	static int  totalRead = 0;
	int32       read = 0;
	float64     data[nSIZE];
	/* Change this variable to 1 if you are using a DSA device and want to check for Overloads. */
	int32       overloadDetectionEnabled = 0;
	bool32      overloaded = 0;
	char        overloadedChannels[nSIZE];

	GLogger<string, ofstream> *file_writer =
		GLogger<string, ofstream>::getInstance();
	////time_t t = time(nullptr);
	////std::string ts = boost::lexical_cast<std::string>(t);

	//boost::posix_time::ptime ptime = boost::posix_time::second_clock::local_time();
	//std::string str_ptime = to_simple_string(ptime); // to_iso_extended_string(ptime);
	//// Use a facet to display time in a custom format (only hour and minutes).
	//boost::posix_time::time_facet* facet = new boost::posix_time::time_facet();
	//facet->format("%yyyy %MM %dd %HH:%mm:%ss"); //yyyy / MM / dd HH : mm:ss.SSS"
	//std::stringstream stream;
	//stream.imbue(std::locale(std::locale::classic(), facet));

	//str_ptime = stream.str();

//	for (size_t i = 0; i < 100; i++) {
	long long timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	string str_ptime = to_string(timestamp);

	//cout << str_ptime << endl;
	//Sleep(100);

//}
//getchar();

//return 0;

//TransferServiceClient *client = (TransferServiceClient *)(callbackData);
	GTaskQue<ThriftMessage, ThriftRWServiceClient> *taskque = (GTaskQue<ThriftMessage, ThriftRWServiceClient> *)(callbackData);

	/*********************************************/
	// DAQmx Read Code
	/*********************************************/
	//	printf("Read\n");
	float64 timeout = 10.0;
	DAQmxErrChk(DAQmxReadAnalogF64(taskHandle, nSAMPLES, timeout, DAQmx_Val_GroupByScanNumber, data, nSIZE, &read, NULL));
	//	printf("Read Done\n");
	if (overloadDetectionEnabled) {
		DAQmxErrChk(DAQmxGetReadOverloadedChansExist(taskHandle, &overloaded));
	}

	//using namespace std::chrono;
	//milliseconds ms = duration_cast< milliseconds >(
	//	system_clock::now().time_since_epoch()
	//	);

	if (read > 0) {
		totalRead += read;
		//		printf("%I64d \n", ms.count());
		//		int count = nSAMPLES / nCHANNEL;
		ThriftMessage message0;
		message0._sender_id = "d0";
		message0._receiver_id = "";
		message0._list_double.reserve(nSIZE);
		message0._timestamp = str_ptime;
		ThriftMessage message1;
		message1._sender_id = "d1";
		message1._receiver_id = "";
		message1._list_double.reserve(nSIZE);
		message1._timestamp = str_ptime;
		ThriftMessage message2;
		message2._sender_id = "d2";
		message2._receiver_id = "";
		message2._list_double.reserve(nSIZE);
		message2._timestamp = str_ptime;
		ThriftMessage message3;
		message3._sender_id = "d3";
		message3._receiver_id = "";
		message3._list_double.reserve(nSIZE);
		message3._timestamp = str_ptime;
		//printf("%s \n", str_ptime);
		//string str="";

		cout << timestamp << ", " << str_ptime << endl;
		for (int i = 0; i < nSIZE; i = i + nCHANNEL) {
			////file_writer->write("%d, %.5f, %.5f, %.5f, %.5f", (long)t, (float)data[i + 0], (float)data[i + 1], (float)data[i + 2], (float)data[i + 3]);
			//string str_time = std::to_string(t);
			//string str_data0 = std::to_string(data[i + 0]);
			//string str_data1 = std::to_string(data[i + 1]);
			//string str_data2 = std::to_string(data[i + 2]);
			//string str_data3 = std::to_string(data[i + 3]);
			//str = str_time + ", " + str_data0 + ", " + str_data1 + ", " + str_data2 + ", " + str_data3 + "\n";
			message0._list_double.push_back(data[i + 0]);
			message1._list_double.push_back(data[i + 1]);
			message2._list_double.push_back(data[i + 2]);
			message3._list_double.push_back(data[i + 3]);
		}
		//		cout << str << endl;
		try {
			//client->writeMessage(message0);
			//client->writeMessage(message1);
			//client->writeMessage(message2);
			//client->writeMessage(message3);
			taskque->pushBack(message0);
			taskque->pushBack(message1);
			taskque->pushBack(message2);
			taskque->pushBack(message3);
		}
		catch (string e) {
			cout << e << endl;
			fflush(stdout);
			goto Error;
		}

		//	printf("Acquired %d samples. Total %d, Data[0] %2.5f, Data[1] %2.5f, Data[2] %2.5f, Data[3] %2.5f \r", (int)read, (int)totalRead, fabs(data[0]), fabs(data[1]), fabs(data[2]), fabs(data[3]));
	}
	//	printf("%d\n", i);
	//	file_writer->write("");

	if (overloaded) {
		DAQmxErrChk(DAQmxGetReadOverloadedChans(taskHandle, overloadedChannels, nSIZE));
		printf("Overloaded channels: %s\n", overloadedChannels);
	}
	fflush(stdout);

Error:
	if (DAQmxFailed(error)) {
		DAQmxGetExtendedErrorInfo(errBuff, 2048);
		/*********************************************/
		// DAQmx Stop Code
		/*********************************************/
		DAQmxStopTask(taskHandle);
		DAQmxClearTask(taskHandle);
		printf("DAQmx Error: %s\n", errBuff);
	}
	return 0;
}

int32 CVICALLBACK DoneCallback(TaskHandle taskHandle, int32 status, void *callbackData)
{
	int32   error = 0;
	char    errBuff[2048] = { '\0' };

	// Check to see if an error stopped the task.
	DAQmxErrChk(status);

Error:
	if (DAQmxFailed(error)) {
		DAQmxGetExtendedErrorInfo(errBuff, 2048);
		DAQmxClearTask(taskHandle);
		printf("DAQmx Error: %s\n", errBuff);
	}
	return 0;
}
