import { addAttachment } from '/script/features/media/attachmentManager.js';
import { t, applyGeneralTranslations } from '/script/core/localization.js';
  

let mediaRecorder;
let audioChunks = [];
let audioBlob;
let audioUrl;
let mediaStream;
let audioContext;
let analyser;
let animationFrameId;
const rings = [];
let rps = 10;

/**
 * Erstellt und zeigt einen Bestätigungsdialog innerhalb des Recorder-Modals an.
 * @param {string} message Die anzuzeigende Frage (z. B. "Sind Sie sicher?").
 * @param {function} onConfirm Die Funktion, die ausgeführt wird, wenn der Benutzer bestätigt.
 */
function showConfirmationDialog(message, onConfirm) {
    // Entferne einen eventuell bereits existierenden Dialog
    const existingDialog = document.querySelector('.confirmation-dialog-overlay');
    if (existingDialog) {
        existingDialog.remove();
    }

    const dialogOverlay = document.createElement('div');
    dialogOverlay.className = 'confirmation-dialog-overlay';

	dialogOverlay.innerHTML = `
	        <div class="confirmation-dialog">
	            <p>${message}</p>
	            <div class="confirmation-dialog-buttons">
	                <button class="recorder-btn text-only" id="confirm-cancel-btn" data-i18n="global.cancel">Cancel</button>
	                <button class="recorder-btn confirmation-btn-confirm" id="confirm-confirm-btn" data-i18n="global.confirm">Confirm</button>
	            </div>
	        </div>
	    `;
	applyGeneralTranslations(dialogOverlay);
		
    const recorderModal = document.querySelector('.recorder-modal');
    if (recorderModal) {
        recorderModal.appendChild(dialogOverlay);
    } else {
        console.error("Konnte das Recorder-Modal nicht finden, um den Dialog anzuhängen.");
        return;
    }

    const cleanup = () => dialogOverlay.remove();
    const confirmBtn = document.getElementById('confirm-confirm-btn');
    const cancelBtn = document.getElementById('confirm-cancel-btn');

    cancelBtn.addEventListener('click', cleanup);
    confirmBtn.addEventListener('click', () => {
        onConfirm();
        cleanup();
    });
}


export function openAudioRecorder() {
    const modal = createModalElement();
    document.body.appendChild(modal);
	applyGeneralTranslations(modal);

    const overlay = document.querySelector('.recorder-overlay');
    const startBtn = document.getElementById('start-btn');
    const stopBtn = document.getElementById('stop-btn');
    const rerecordBtn = document.getElementById('rerecord-btn');
    const addBtn = document.getElementById('add-btn');
    const closeBtn = document.querySelector('.recorder-close-btn');
    const audioPlayer = document.querySelector('.audio-player');

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
        const canvas = document.getElementById('modal-visualizer');
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
        overlay.remove();
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
                audioChunks = [];
                mediaRecorder = new MediaRecorder(stream, { mimeType: 'audio/webm' });
                mediaRecorder.addEventListener("dataavailable", e => audioChunks.push(e.data));
                mediaRecorder.addEventListener("stop", () => {
                    audioBlob = new Blob(audioChunks, { type: 'audio/webm' });
                    audioUrl = URL.createObjectURL(audioBlob);
                    audioPlayer.src = audioUrl;
                    setModalState('reviewing');
                });
                initVisualizer(stream);
                mediaRecorder.start();
                setModalState('recording');
            })
            .catch(err => {
                alert(t('recorder.error_access'));
                console.error(err);
                closeModal();
            });
    }

    setTimeout(() => overlay.classList.add('is-visible'), 10);
}


function initVisualizer(stream) {
    audioContext = new (window.AudioContext || window.webkitAudioContext)();
    analyser = audioContext.createAnalyser();
    const source = audioContext.createMediaStreamSource(stream);
    source.connect(analyser);

    analyser.fftSize = 256;
    const bufferLength = analyser.frequencyBinCount;
    const dataArray = new Uint8Array(bufferLength);

    const canvas = document.getElementById('modal-visualizer');
    const ctx = canvas.getContext('2d');

    canvas.width = canvas.clientWidth;
    canvas.height = canvas.clientHeight;

    const recordIcon = document.querySelector('.record-icon');
    const iconRect = recordIcon.getBoundingClientRect();
    const canvasRect = canvas.getBoundingClientRect();

    const centerX = (iconRect.left - canvasRect.left) + (iconRect.width / 2);
    const centerY = (iconRect.top - canvasRect.top) + (iconRect.height / 2);

    let lastRingTime = 0;

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

function createModalElement() {
    const overlay = document.createElement('div');
    overlay.className = 'recorder-overlay';
    overlay.innerHTML = `
        <div class="recorder-modal is-initial">
            <canvas id="modal-visualizer"></canvas> 
            <div class="recorder-header">
                <h2 data-i18n="recorder.title">Record new audio</h2>
                <button class="recorder-close-btn">×</button>
            </div>
            <div class="record-icon">
                <div class="record-icon-inner"></div>
            </div>

            <div class="recorder-body">
                <audio class="audio-player" controls></audio>
            </div>
            <div class="recorder-footer">
                <span class="footer-text" data-i18n="recorder.footer_text">Explain your idea.</span>
                <div class="footer-buttons">
                    <div class="initial-controls">
                        <button id="start-btn" class="recorder-btn" data-i18n="recorder.btn_start">Start recording</button>
                    </div>
                    <div class="recording-controls">
                        <button id="stop-btn" class="recorder-btn" data-i18n="recorder.btn_stop">Stop recording</button>
                    </div>
                    <div class="review-controls">
                        <button id="rerecord-btn" class="recorder-btn text-only" data-i18n="recorder.btn_rerecord">Re-record</button>
                        <button id="add-btn" class="recorder-btn" data-i18n="recorder.btn_add">Add to prompt</button>
                    </div>
                </div>
            </div>
        </div>
    `;
    return overlay;
}
