import { addAttachment } from '/script/features/media/attachmentManager.js';
import { t, applyGeneralTranslations } from '/script/core/localization.js';
import { renderTemplate, getTemplate } from '/script/core/templateManager.js';
  
let mediaRecorder;
let audioChunks =[];
let audioBlob;
let audioUrl;
let mediaStream;
let audioContext;
let analyser;
let animationFrameId;
const rings =[];
let rps = 10;

/**
 * Erstellt und zeigt einen Bestätigungsdialog innerhalb des Recorder-Modals an.
 * @param {string} message Die anzuzeigende Frage (z. B. "Sind Sie sicher?").
 * @param {function} onConfirm Die Funktion, die ausgeführt wird, wenn der Benutzer bestätigt.
 */
function showConfirmationDialog(message, onConfirm) {
    // 1. Altes Dialogfeld entfernen
    const existingDialog = document.querySelector('#confirmation-dialog-overlay');
    if (existingDialog) {
        existingDialog.remove();
    }

    // 2. Template holen (nicht rendern, da es nicht in den body soll!)
    const fragment = getTemplate('tpl-confirmation-dialog-overlay');
    if (!fragment) return;

    // Das Wurzelelement aus dem Fragment extrahieren
    const dialogOverlay = fragment.firstElementChild; 

    // 3. Dynamischen Inhalt und Übersetzungen anwenden
    dialogOverlay.querySelector('.confirm-msg').textContent = message;
    applyGeneralTranslations(dialogOverlay);

    // 4. In das Elternelement (das Recorder-Modal) einfügen
    const recorderModal = document.querySelector('.recorder-modal');
    if (recorderModal) {
        recorderModal.appendChild(dialogOverlay);
    } else {
        console.error("Konnte das Recorder-Modal nicht finden, um den Dialog anzuhängen.");
        return;
    }

    // 5. Events binden
    const cleanup = () => dialogOverlay.remove();
    const confirmBtn = dialogOverlay.querySelector('.confirm-confirm-btn');
    const cancelBtn = dialogOverlay.querySelector('.confirm-cancel-btn');

    cancelBtn.addEventListener('click', cleanup);
    confirmBtn.addEventListener('click', () => {
        onConfirm();
        cleanup();
    });
}

export function openAudioRecorder() {
    // 1. Erstellen, einfügen und übersetzen mit einem Aufruf
    const overlay = renderTemplate('audio-recorder-overlay');
    if (!overlay) return;

    // 2. Events nur einmal binden (Da wir remove() nutzen, ist es bei jedem neuen Aufruf false)
    if (overlay.dataset.eventsAttached !== 'true') {
        const startBtn = overlay.querySelector('#start-btn');
        const stopBtn = overlay.querySelector('#stop-btn');
        const rerecordBtn = overlay.querySelector('#rerecord-btn');
        const addBtn = overlay.querySelector('#add-btn');
        const closeBtn = overlay.querySelector('.recorder-close-btn');
        const audioPlayer = overlay.querySelector('.audio-player');

        function shutdownRecordingResources() {
            if (animationFrameId) {
                cancelAnimationFrame(animationFrameId);
                animationFrameId = null;
            }
            if (audioContext && audioContext.state !== 'closed') {
                audioContext.close();
            }
            if (mediaStream) {
                mediaStream.getTracks().forEach(track => track.stop());
                console.log("Mikrofon-Stream erfolgreich geschlossen.");
            }
            const canvas = overlay.querySelector('#modal-visualizer');
            if (canvas) {
                const ctx = canvas.getContext('2d');
                ctx.clearRect(0, 0, canvas.width, canvas.height);
            }
        }

        function closeModal() {
            shutdownRecordingResources();
            if (mediaRecorder && mediaRecorder.state === "recording") {
                mediaRecorder.stop();
            }
            if (audioUrl) {
                URL.revokeObjectURL(audioUrl);
            }
            // Wir entfernen das Element sauber aus dem DOM
            overlay.remove();
        }

        function setModalState(state) {
            if (state === 'initial') {
                audioPlayer.src = '';
                if (audioUrl) {
                    URL.revokeObjectURL(audioUrl);
                    audioUrl = null;
                }
            }
            overlay.querySelector('.recorder-modal').className = `recorder-modal is-${state}`;
        }

        function startRecording() {
            navigator.mediaDevices.getUserMedia({ audio: true })
                .then(stream => {
                    mediaStream = stream;
                    audioChunks =[];
                    mediaRecorder = new MediaRecorder(stream, { mimeType: 'audio/webm' });
                    mediaRecorder.addEventListener("dataavailable", e => audioChunks.push(e.data));
                    mediaRecorder.addEventListener("stop", () => {
                        audioBlob = new Blob(audioChunks, { type: 'audio/webm' });
                        audioUrl = URL.createObjectURL(audioBlob);
                        audioPlayer.src = audioUrl;
                        setModalState('reviewing');
                    });
                    initVisualizer(stream, overlay);
                    mediaRecorder.start();
                    setModalState('recording');
                })
                .catch(err => {
                    alert(t('recorder.error_access'));
                    console.error(err);
                    closeModal();
                });
        }

        stopBtn.addEventListener('click', () => {
            if (mediaRecorder && mediaRecorder.state === "recording") {
                mediaRecorder.stop();
            }
            shutdownRecordingResources();
        });

        startBtn.addEventListener('click', startRecording);

        rerecordBtn.addEventListener('click', () => {
            showConfirmationDialog(
                t('recorder.confirm_rerecord'),
                () => setModalState('initial')
            );
        });

        addBtn.addEventListener('click', () => {
            const fileName = `${t('recorder.filename_prefix')}${new Date().toLocaleString()}.webm`;
            const audioFile = new File([audioBlob], fileName, { type: audioBlob.type, lastModified: Date.now() });

            addAttachment(audioFile, 'audio');
            closeModal();
        });
        
        closeBtn.addEventListener('click', closeModal);

        overlay.dataset.eventsAttached = 'true';
    }

    // 3. Status zurücksetzen und anzeigen
    overlay.querySelector('.recorder-modal').className = `recorder-modal is-initial`;
    setTimeout(() => overlay.classList.add('is-visible'), 10);
}


function initVisualizer(stream, overlay) {
    audioContext = new (window.AudioContext || window.webkitAudioContext)();
    analyser = audioContext.createAnalyser();
    const source = audioContext.createMediaStreamSource(stream);
    source.connect(analyser);

    analyser.fftSize = 256;
    const bufferLength = analyser.frequencyBinCount;
    const dataArray = new Uint8Array(bufferLength);

    // Canvas aus dem übergebenen Overlay holen
    const canvas = overlay.querySelector('#modal-visualizer');
    const ctx = canvas.getContext('2d');

    canvas.width = canvas.clientWidth;
    canvas.height = canvas.clientHeight;

    const recordIcon = overlay.querySelector('.record-icon');
    const iconRect = recordIcon.getBoundingClientRect();
    const canvasRect = canvas.getBoundingClientRect();

    const centerX = (iconRect.left - canvasRect.left) + (iconRect.width / 2);
    const centerY = (iconRect.top - canvasRect.top) + (iconRect.height / 2);

    let lastRingTime = 0;
    rings.length = 0; // Array leeren beim Neustart

    function draw() {
        animationFrameId = requestAnimationFrame(draw);

        analyser.getByteFrequencyData(dataArray);
        const volume = dataArray.reduce((a, b) => a + b) / bufferLength;

        ctx.clearRect(0, 0, canvas.width, canvas.height);

        const currentTime = Date.now();

        if (volume > 40 && (currentTime - lastRingTime > 1000 / rps)) {
            rings.push({
                spawnTime: currentTime,
                initialVolume: volume,
            });
            lastRingTime = currentTime;
        }

        for (let i = rings.length - 1; i >= 0; i--) {
            const ring = rings[i];
            const age = currentTime - ring.spawnTime;

            const maxLifetime = (ring.initialVolume * 10);
            const lineWidth = ring.initialVolume / 10;
            const opacity = 0.80 - (age / maxLifetime);

            if (opacity <= 0) {
                rings.splice(i, 1);
                continue;
            }

            const radius = age * 0.20;

            ctx.beginPath();
            ctx.arc(centerX, centerY, radius, 0, 2 * Math.PI);
            ctx.strokeStyle = `rgba(202, 3, 0, ${opacity})`;
            ctx.lineWidth = lineWidth;

            ctx.shadowBlur = 8;
            ctx.shadowColor = ctx.strokeStyle;

            ctx.stroke();

            ctx.shadowBlur = 0;
        }
    }
    draw();
}