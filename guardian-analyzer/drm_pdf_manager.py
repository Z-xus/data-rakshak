import fitz
import json
import os
import uuid
import requests
from datetime import datetime
from typing import Dict, Any, Optional
from cryptography.fernet import Fernet
import logging

logger = logging.getLogger("guardian-analyzer")

class DRMPDFManager:
    def __init__(self, api_url: str):
        """Initialize DRM PDF Manager"""
        self.api_url = api_url
        self.key = Fernet.generate_key()
        self.cipher_suite = Fernet(self.key)
        
    def create_drm_pdf(
        self,
        input_path: str,
        output_path: str,
        owner_id: str,
        expiry_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Create a stealthy DRM-protected PDF that looks like a normal PDF"""
        try:
            doc_id = str(uuid.uuid4())
            
            # Create a more subtle verification script
            verify_script = f"""
            try {{
                function checkAccess() {{
                    var xhr = new XMLHttpRequest();
                    xhr.open('GET', '{self.api_url}/verify?doc_id={doc_id}', false);
                    xhr.send(null);
                    
                    if (xhr.status !== 200) {{
                        // Gradually fade out content
                        var pages = this.numPages;
                        for (var i = 0; i < pages; i++) {{
                            this.setPageContent(i, "");  // Clear page content
                        }}
                        this.dirty = true;
                        this.reload();
                    }}
                }}
                
                // Check periodically without showing alerts
                setInterval(checkAccess, 30000);  // Every 30 seconds
                
                // Also check when document gains focus
                this.setFocus = function() {{
                    checkAccess();
                    this._setFocus.apply(this, arguments);
                }}
            }} catch (e) {{
                // Fail silently to avoid detection
                console.log(e);
            }}
            """
            
            # Open and modify the PDF
            doc = fitz.open(input_path)
            
            # Add hidden metadata for tracking
            doc.set_metadata({
                "format": "PDF-1.7",  # Standard PDF format
                "encryption": None,    # Hide encryption info
                "javascript": verify_script,
                "drm_id": doc_id      # Hidden DRM ID
            })
            
            # Save with minimal visible security
            doc.save(
                output_path,
                encryption=fitz.PDF_ENCRYPT_AES_256,
                owner_pw=self.key.decode(),
                user_pw="",  # No visible password protection
                permissions=fitz.PDF_PERM_ACCESSIBILITY | fitz.PDF_PERM_PRINT | fitz.PDF_PERM_COPY,
                garbage=4,   # Maximum cleanup
                deflate=True # Compress to hide modifications
            )
            
            # Store DRM metadata separately
            drm_metadata = {
                'doc_id': doc_id,
                'owner_id': owner_id,
                'creation_date': datetime.now().isoformat(),
                'expiry_date': expiry_date.isoformat() if expiry_date else None,
                'status': 'active',
                'original_hash': self._calculate_hash(input_path)
            }
            
            self._store_metadata(doc_id, drm_metadata)
            
            return {
                'status': 'success',
                'doc_id': doc_id,
                'output_path': output_path
            }
            
        except Exception as e:
            logger.error(f"Error creating stealth DRM PDF: {str(e)}")
            raise

    def revoke_access(self, doc_id: str, owner_id: str) -> Dict[str, Any]:
        """Revoke access to a DRM-protected PDF"""
        try:
            # Update DRM status on server
            response = requests.post(f"{self.api_url}/revoke", 
                json={'doc_id': doc_id, 'owner_id': owner_id})
            
            if response.status_code == 200:
                return {'status': 'success', 'message': 'Access revoked successfully'}
            else:
                raise Exception(f"Failed to revoke access: {response.text}")
                
        except Exception as e:
            logger.error(f"Error revoking access: {str(e)}")
            raise

    def open_drm_pdf(self, drm_path: str) -> fitz.Document:
        """Open and verify a DRM-protected PDF"""
        try:
            # Read DRM container
            with open(drm_path, 'r') as f:
                drm_container = json.load(f)
            
            metadata = drm_container['metadata']
            
            # Verify access with server
            response = requests.get(f"{self.api_url}/verify",
                params={'doc_id': metadata['doc_id']})
            
            if response.status_code != 200:
                raise Exception("Access denied or revoked")
            
            # Decrypt content
            cipher_suite = Fernet(metadata['encryption_key'].encode())
            decrypted_data = cipher_suite.decrypt(drm_container['content'].encode())
            
            # Create temporary file for PyMuPDF
            temp_path = f"/tmp/{metadata['doc_id']}.pdf"
            with open(temp_path, 'wb') as f:
                f.write(decrypted_data)
            
            # Open with PyMuPDF
            doc = fitz.open(temp_path)
            os.remove(temp_path)  # Clean up
            
            return doc
            
        except Exception as e:
            logger.error(f"Error opening DRM PDF: {str(e)}")
            raise 

    def _calculate_hash(self, file_path: str) -> str:
        """Calculate file hash for integrity checking"""
        import hashlib
        with open(file_path, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()