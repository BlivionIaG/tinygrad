#include "TinyGPUDriverUserClient.h"
#include "TinyGPUDriver.h"
#include <DriverKit/DriverKit.h>
#include <DriverKit/OSSharedPtr.h>
#include <PCIDriverKit/PCIDriverKit.h>

struct TinyGPUDriverUserClient_IVars
{
	OSSharedPtr<TinyGPUDriver> provider = nullptr;

	TinyGPUCreateDMAResp *dmas = nullptr;
	size_t dmaCount = 0;
	size_t dmaCap = 0;

	int ensureDMACap(size_t need)
	{
		// not thread-safe
		if (need <= dmaCap) return 0;

		size_t newCap = dmaCap ? dmaCap * 2 : 16;
		while (newCap < need) newCap *= 2;

		auto *newArr = IONewZero(TinyGPUCreateDMAResp, newCap);
		if (!newArr) return -kIOReturnNoMemory;

		if (dmas && dmaCount) {
			memcpy(newArr, dmas, dmaCount * sizeof(TinyGPUCreateDMAResp));
		}

		IOSafeDeleteNULL(dmas, TinyGPUCreateDMAResp, dmaCap);
		dmas = newArr;
		dmaCap = newCap;
		return 0;
	}

	// a slot for a new PrepareDMA mapping: the lowest one ReleaseDMA emptied, else a new one at the end (not committed until dmas[slot] is written)
	int findDMASlot(size_t *slot)
	{
		for (size_t i = 0; i < dmaCount; i++) {
			if (!dmas[i].dmaCmd && !dmas[i].sharedBuf) { *slot = i; return 0; }
		}
		*slot = dmaCount;
		return ensureDMACap(dmaCount + 1);
	}
};

// PrepareDMA appends these two words after the (0, 0) that ends its address table: a server that sees the magic knows ReleaseDMA exists and which slot to name
static const uint64_t kReleaseDMAMagic = 0x52454C444D410001ull;

// IOConnectMapMemory64 type of a write-combined BAR: kBarTypeWC | bar
static const uint64_t kBarTypeWC = 0xF0000100ull;

bool TinyGPUDriverUserClient::init()
{
	auto ok = super::init();
	if (!ok) return false;

	ivars = IONewZero(TinyGPUDriverUserClient_IVars, 1);
	if (!ivars) return false;
	return true;
}

void TinyGPUDriverUserClient::free()
{
	if (ivars) {
		IOSafeDeleteNULL(ivars, TinyGPUDriverUserClient_IVars, 1);
	}
	super::free();
}

kern_return_t TinyGPUDriverUserClient::Start_Impl(IOService* in_provider)
{
	kern_return_t err = kIOReturnSuccess;
	if (!in_provider) {
		os_log(OS_LOG_DEFAULT, "tinygpu: provider is null");
		err = kIOReturnBadArgument;
		goto error;
	}

	err = Start(in_provider, SUPERDISPATCH);
	if (err) {
		os_log(OS_LOG_DEFAULT, "tinygpu: failed to start super (%d)", err);
		goto error;
	}

	ivars->provider = OSSharedPtr(OSDynamicCast(TinyGPUDriver, in_provider), OSRetain);
	return 0;

error:
	ivars->provider.reset();
	return err;
}

kern_return_t TinyGPUDriverUserClient::Stop_Impl(IOService* in_provider)
{
	// release all DMA allocations for this client
	if (ivars) {
		for (size_t i = 0; i < ivars->dmaCount; i++) {
			auto &d = ivars->dmas[i];
			if (d.dmaCmd) {
				d.dmaCmd->CompleteDMA(kIODMACommandCompleteDMANoOptions);
				d.dmaCmd->release();
				d.dmaCmd = nullptr;
			}
		}
		ivars->dmaCount = 0;
		IOSafeDeleteNULL(ivars->dmas, TinyGPUCreateDMAResp, ivars->dmaCap);
		ivars->dmas = nullptr;
		ivars->provider.reset();
	}

	return Stop(in_provider, SUPERDISPATCH);
}

kern_return_t TinyGPUDriverUserClient::ExternalMethod(uint64_t selector, IOUserClientMethodArguments* args, const IOUserClientMethodDispatch* in_dispatch, OSObject* in_target, void* in_reference)
{
	kern_return_t err = 0;

	os_log(OS_LOG_DEFAULT, "tinygpu: rpc (%llu) in:%d, out:%d", selector, args->scalarInputCount, args->scalarOutputCount);

	if (selector == TinyGPURPC::ReadCfg) {
		if (args->scalarInputCount != 2 or args->scalarOutputCount < 1) return kIOReturnBadArgument;

		uint32_t off = uint32_t(args->scalarInput[0]);
		uint32_t size = uint32_t(args->scalarInput[1]);

		uint32_t val = 0;
		err = ivars->provider->CfgRead(off, size, &val);
		os_log(OS_LOG_DEFAULT, "tinygpu: read cfg off:%x sz:%d, val:%x", off, size, val);

		if (!err) {
			args->scalarOutput[0] = val;
			args->scalarOutputCount = 1;
		}
		return err;
	} else if (selector == TinyGPURPC::WriteCfg) {
		if (args->scalarInputCount != 3) return kIOReturnBadArgument;

		uint32_t off = uint32_t(args->scalarInput[0]);
		uint32_t size = uint32_t(args->scalarInput[1]);
		uint32_t val = uint32_t(args->scalarInput[2]);

		os_log(OS_LOG_DEFAULT, "tinygpu: wr cfg off:%x sz:%d, val:%x", off, size, val);
		return ivars->provider->CfgWrite(off, size, val);
	} else if (selector == TinyGPURPC::Reset) {
		os_log(OS_LOG_DEFAULT, "tinygpu: reset");
		return ivars->provider->ResetDevice();
	} else if (selector == TinyGPURPC::PrepareDMA) {
		// both input and output buffers must be >= 4097 bytes for IOMemoryDescriptor
		if (!args->structureInputDescriptor || !args->structureOutputDescriptor) {
			os_log(OS_LOG_DEFAULT, "tinygpu: PrepareDMA requires buffers >= 4097 bytes");
			return kIOReturnBadArgument;
		}
		size_t slot = 0;
		if (ivars->findDMASlot(&slot)) return kIOReturnNoMemory;

		uint64_t size = 0;
		args->structureInputDescriptor->GetLength(&size);

		IODMACommand* dmaCmd = nullptr;
		IOAddressSegment segments[32];
		uint32_t segCount = 32;
		err = ivars->provider->SetupDMA(args->structureInputDescriptor, size, &dmaCmd, segments, &segCount);
		if (err) return err;

		// write physical addresses to output: [addr0, len0, addr1, len1, ..., 0, 0]
		IOMemoryMap* outMap = nullptr;
		err = args->structureOutputDescriptor->CreateMapping(0, 0, 0, 0, 0, &outMap);
		if (err || !outMap) { os_log(OS_LOG_DEFAULT, "tinygpu: output map failed err=%d", err); dmaCmd->release(); return err; }

		uint64_t* out = (uint64_t*)outMap->GetAddress();
		for (uint32_t i = 0; i < segCount; i++) { out[i * 2] = segments[i].address; out[i * 2 + 1] = segments[i].length; }
		out[segCount * 2] = 0; out[segCount * 2 + 1] = 0;
		out[segCount * 2 + 2] = kReleaseDMAMagic; out[segCount * 2 + 3] = slot;
		outMap->release();

		os_log(OS_LOG_DEFAULT, "tinygpu: PrepareDMA size=%llu segs=%u slot=%zu", size, segCount, slot);
		ivars->dmas[slot] = {nullptr, dmaCmd};
		if (slot == ivars->dmaCount) ivars->dmaCount++;
		return kIOReturnSuccess;
	} else if (selector == TinyGPURPC::ReleaseDMA) {
		// scalar in[0] = the slot PrepareDMA reported: gives the mapping back (DART unmap). The caller guarantees the device no longer uses it.
		if (args->scalarInputCount != 1) return kIOReturnBadArgument;
		uint64_t slot = args->scalarInput[0];
		if (slot >= ivars->dmaCount) return kIOReturnBadArgument;
		auto &d = ivars->dmas[slot];
		if (!d.dmaCmd || d.sharedBuf) return kIOReturnBadArgument; // empty, or made by CreateDMA (those live until the client closes)
		err = d.dmaCmd->CompleteDMA(kIODMACommandCompleteDMANoOptions);
		if (err) { os_log(OS_LOG_DEFAULT, "tinygpu: ReleaseDMA slot=%llu failed err=%d", slot, err); return err; } // still mapped, still ours
		d.dmaCmd->release();
		d.dmaCmd = nullptr;
		os_log(OS_LOG_DEFAULT, "tinygpu: ReleaseDMA slot=%llu", slot);
		return kIOReturnSuccess;
	} else if (selector == TinyGPURPC::ResetLink) {
		// scalar in = [type, options] of IOPCIDevice::Reset on the GPU's nub (everything below its parent bridge is reset); the kern_return of Reset comes back unchanged
		if (args->scalarInputCount != 2) return kIOReturnBadArgument;
		return ivars->provider->ResetLink(uint32_t(args->scalarInput[0]), uint32_t(args->scalarInput[1]));
	} else if (selector == TinyGPURPC::Features) {
		// scalar out[0] = feature bits of this dext: bit 0 = a BAR can be mapped write-combined (user-client memory type kBarTypeWC | bar)
		if (args->scalarOutputCount < 1) return kIOReturnBadArgument;
		args->scalarOutput[0] = 1;
		args->scalarOutputCount = 1;
		return kIOReturnSuccess;
	}

	return kIOReturnUnsupported;
}

kern_return_t IMPL(TinyGPUDriverUserClient, CopyClientMemoryForType)
{
	if (!memory) return kIOReturnBadArgument;
	if (!ivars->provider.get()) return kIOReturnNotAttached;

	// bar handling, type is bar num; the same BAR write-combined is kBarTypeWC | bar (above every DMA size, which the DART caps at 2047 MiB)
	if (type < 6) {
		uint32_t bar = (uint32_t)type;
		return ivars->provider->MapBar(bar, false, memory);
	}
	if ((type & ~0xFFull) == kBarTypeWC) {
		uint32_t bar = (uint32_t)(type & 0xFF);
		if (bar >= 6) return kIOReturnBadArgument;
		return ivars->provider->MapBar(bar, true, memory);
	}

	// dma handling, type is size
	if (ivars->ensureDMACap(ivars->dmaCount + 1)) {
		os_log(OS_LOG_DEFAULT, "tinygpu: cannot grow dma array");
		return kIOReturnNoMemory;
	}

	TinyGPUCreateDMAResp buf{};
	kern_return_t err = ivars->provider->CreateDMA(type, &buf);
	if (err) return err;

	ivars->dmas[ivars->dmaCount++] = buf;
	*memory = buf.sharedBuf;
	return 0;
}
