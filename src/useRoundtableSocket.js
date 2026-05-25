import { useCallback, useEffect, useRef, useState } from 'react';

const WS_PATH = '/ws';

export function useRoundtableSocket({ meetingId, onEvent }) {
  const socketRef = useRef(null);
  const onEventRef = useRef(onEvent);
  const [status, setStatus] = useState('connecting');

  useEffect(() => {
    onEventRef.current = onEvent;
  }, [onEvent]);

  useEffect(() => {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const socket = new WebSocket(`${protocol}//${window.location.host}${WS_PATH}`);
    let closedByCleanup = false;
    socketRef.current = socket;
    setStatus('connecting');

    socket.addEventListener('open', () => setStatus('open'));
    socket.addEventListener('close', () => {
      if (socketRef.current === socket) {
        socketRef.current = null;
      }
      setStatus(closedByCleanup ? 'connecting' : 'closed');
    });
    socket.addEventListener('error', () => setStatus('error'));
    socket.addEventListener('message', (event) => {
      try {
        const payload = JSON.parse(event.data);
        if (!payload || typeof payload !== 'object') {
          throw new Error('invalid payload');
        }
        if (payload.meetingId && payload.meetingId !== meetingId) {
          return;
        }
        onEventRef.current?.(payload);
      } catch {
        onEventRef.current?.({ type: 'error', message: '收到无法解析的消息' });
      }
    });

    return () => {
      closedByCleanup = true;
      socket.close();
      if (socketRef.current === socket) {
        socketRef.current = null;
      }
    };
  }, [meetingId]);

  const send = useCallback((payload) => {
    if (socketRef.current?.readyState !== WebSocket.OPEN) {
      return false;
    }

    socketRef.current.send(JSON.stringify(payload));
    return true;
  }, []);

  return { status, send };
}
