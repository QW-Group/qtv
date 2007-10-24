/*
	msg.c
*/

#include "qtv.h"

void InitNetMsg(netmsg_t *b, char *buffer, int bufferlength)
{
	b->data = buffer;
	b->maxsize = bufferlength;
	b->readpos = 0;
	b->cursize = 0;
}

//probably not the place for these any more..
unsigned char ReadByte(netmsg_t *b)
{
	if (b->readpos >= b->cursize)
	{
		b->readpos = b->cursize+1;
		return 0;
	}
	return b->data[b->readpos++];
}

unsigned short ReadShort(netmsg_t *b)
{
	int b1, b2;
	b1 = ReadByte(b);
	b2 = ReadByte(b);

	return b1 | (b2<<8);
}

unsigned int ReadLong(netmsg_t *b)
{
	int s1, s2;
	s1 = ReadShort(b);
	s2 = ReadShort(b);

	return s1 | (s2<<16);
}

unsigned int BigLong(unsigned int val)
{
	union {
		unsigned int i;
		unsigned char c[4];
	} v;

	v.i = val;
	return (v.c[0]<<24) | (v.c[1] << 16) | (v.c[2] << 8) | (v.c[3] << 0);
}

unsigned int SwapLong(unsigned int val)
{
	union {
		unsigned int i;
		unsigned char c[4];
	} v;
	unsigned char s;

	v.i = val;
	s = v.c[0];
	v.c[0] = v.c[3];
	v.c[3] = s;
	s = v.c[1];
	v.c[1] = v.c[2];
	v.c[2] = s;

	return v.i;
}

float ReadFloat(netmsg_t *b)
{
	union {
		unsigned int i;
		float f;
	} u;

	u.i = ReadLong(b);
	return u.f;
}

void ReadString(netmsg_t *b, char *string, int maxlen)
{
	maxlen--;	//for null terminator
	while(maxlen)
	{
		*string = ReadByte(b);
		if (!*string)
			return;
		string++;
		maxlen--;
	}
	*string++ = '\0';	//add the null
}

void WriteByte(netmsg_t *b, unsigned char c)
{
	if (b->cursize>=b->maxsize)
		return;
	b->data[b->cursize++] = c;
}

void WriteShort(netmsg_t *b, unsigned short l)
{
	WriteByte(b, (l&0x00ff)>>0);
	WriteByte(b, (l&0xff00)>>8);
}

void WriteLong(netmsg_t *b, unsigned int l)
{
	WriteByte(b, (l&0x000000ff)>>0);
	WriteByte(b, (l&0x0000ff00)>>8);
	WriteByte(b, (l&0x00ff0000)>>16);
	WriteByte(b, (l&0xff000000)>>24);
}

void WriteFloat(netmsg_t *b, float f)
{
	union {
		unsigned int i;
		float f;
	} u;

	u.f = f;
	WriteLong(b, u.i);
}

void WriteString2(netmsg_t *b, const char *str)
{	//no null terminator, convienience function.
	while(*str)
		WriteByte(b, *str++);
}

void WriteString(netmsg_t *b, const char *str)
{
	while(*str)
		WriteByte(b, *str++);
	WriteByte(b, 0);
}

void WriteData(netmsg_t *b, const char *data, int length)
{
	int i;
	unsigned char *buf;

	if (b->cursize + length > b->maxsize)	//urm, that's just too big. :(
		return;
	buf = b->data+b->cursize;
	for (i = 0; i < length; i++)
		*buf++ = *data++;
	b->cursize+=length;
}
